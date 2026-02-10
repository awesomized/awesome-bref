<?php

declare(strict_types=1);

namespace Bref\FpmRuntime;

use Bref\Context\Context;
use Bref\Event\Http\FastCgi\FastCgiCommunicationFailed;
use Bref\Event\Http\FastCgi\FastCgiRequest;
use Bref\Event\Http\FastCgi\Timeout;
use Bref\Event\Http\HttpHandler;
use Exception;
use hollodotme\FastCGI\Client;
use hollodotme\FastCGI\Exceptions\TimedoutException;
use hollodotme\FastCGI\Interfaces\ProvidesRequestData;
use hollodotme\FastCGI\Interfaces\ProvidesResponseData;
use hollodotme\FastCGI\SocketConnections\UnixDomainSocket;
use Symfony\Component\Process\Process;
use Throwable;

/**
 * Handles HTTP events coming from API Gateway/ALB by proxying them to PHP-FPM via FastCGI.
 *
 * This is a copy-and-paste from FpmHandler, with modifications to support lambdas running on managed images, where a
 * single process may handle concurrent requests. Primarily, this means that this class accepts that PHP-FPM may
 * already be running and will not attempt to manage lifecycle of the process unless it was the process that started it.
 * A non-owning instance will manage its own connection to php-fpm only
 *
 * Note that lambda function concurrency must be aligned with the php-fpm max_clients configuration. There must be
 * an equal or higher number of php-fpm children than the maximum number of concurrent lambda executions. This is not
 * enforced at all by this code
 */
final class ConcurrentFpmHandler extends HttpHandler
{
    private const SOCKET = '/tmp/.bref/php-fpm.sock';
    private const CONFIG = '/opt/bref/etc/php-fpm.conf';
    private const LOCK_FILE = '/tmp/.bref/php-fpm.lock';

    /** @var Client|null */
    private $client;
    /** @var UnixDomainSocket */
    private $connection;
    /** @var string */
    private $handler;
    /** @var string */
    private $configFile;
    /** @var Process|null */
    private $fpm;

    public function __construct(string $handler, string $configFile = self::CONFIG)
    {
        $this->handler = $handler;
        $this->configFile = $configFile;
    }

    /**
     * Start the PHP-FPM process.
     */
    public function start(): void
    {
        if (!is_dir(dirname(self::LOCK_FILE))) {
            mkdir(dirname(self::LOCK_FILE));
        }

        $lockfile = fopen(self::LOCK_FILE, 'w');

        // wait until we can acquire an exclusive lock before starting. this is to ensure that a single php
        // process has ownership of a single php-fpm process
        flock($lockfile, LOCK_EX);

        // if php-fpm is not running, we have a lock and can start up now
        if (!$this->isReady()) {
            if (! is_dir(dirname(self::SOCKET))) {
                mkdir(dirname(self::SOCKET));
            }

            echo "starting new fpm process - " . getmypid() . "\n";

            /**
             * --nodaemonize: we want to keep control of the process
             * --force-stderr: force logs to be sent to stderr, which will allow us to send them to CloudWatch
             */
            $this->fpm = new Process(['php-fpm', '--nodaemonize', '--force-stderr', '--fpm-config', $this->configFile]);

            $this->fpm->setTimeout(null);
            $this->fpm->start(function ($type, $output): void {
                // Send any PHP-FPM log to CloudWatch
                echo $output;
            });
        }

        $this->waitUntilReady();

        // whether we started php-fpm or not, we create a client connection for this instance
        $this->client = new Client;
        $this->connection = new UnixDomainSocket(self::SOCKET, 1000, 900000);

        // release the lock
        flock($lockfile, LOCK_UN);
        fclose($lockfile);
    }

    public function stop(): void
    {
        if ($this->fpm && $this->fpm?->isRunning()) {
            // Give it less than a second to stop (500ms should be plenty enough time)
            // this is for the case where the script timed out: we reserve 1 second before the end
            // of the Lambda timeout, so we must kill everything and restart FPM in 1 second.
            // Note: Symfony will first try sending SIGTERM (15) and then SIGKILL (9)
            $this->fpm->stop(0.5);
            if ($this->isReady()) {
                throw new Exception('PHP-FPM cannot be stopped');
            }
        }
    }

    public function __destruct()
    {
        $this->stop();
    }

    /**
     * Proxy the API Gateway event to PHP-FPM and return its response.
     */
    public function handleRequest(HttpRequestEvent $event, Context $context): HttpResponse
    {
        $request = $this->eventToFastCgiRequest($event, $context);

        // The script will timeout 1 second before the remaining time
        // to allow some time for Bref/PHP-FPM to recover and cleanup
        $margin = 1000;
        $timeoutDelayInMs = max(1000, $context->getRemainingTimeInMillis() - $margin);

        try {
            $socketId = $this->client->sendAsyncRequest($this->connection, $request);

            $response = $this->client->readResponse($socketId, $timeoutDelayInMs);
        } catch (TimedoutException $e) {
            echo "The PHP script timed out. Bref will now restart PHP-FPM to start from a clean slate and flush the PHP logs.\nTimeouts can happen for example when trying to connect to a remote API or database, if this happens continuously check for those.\nIf you are using a RDS database, read this: https://bref.sh/docs/environment/database.html#accessing-the-internet\n";

            /**
             * Restart FPM so that the blocked script is 100% terminated and that its logs are flushed to stderr.
             *
             * - "why restart FPM?": if we don't, the previous request continues to execute on the next request
             * - "why not send a SIGUSR2 signal to FPM?": that was a promising approach because SIGUSR2
             *   causes FPM to cleanly stop the FPM worker that is stuck in a timeout/waiting state.
             *   It also causes all worker logs buffered by FPM to be written to stderr (great!).
             *   This takes a bit of time (a few ms), but it's faster than rebooting FPM entirely.
             *   However, the downside is that it doesn't "kill" the previous request execution:
             *   it merely stops the execution of the line of code that is waiting (e.g. "sleep()",
             *   "file_get_contents()", ...) and continues to the next line. That's super weird!
             *   So SIGUSR2 isn't a great solution in the end.
             */
            $this->stop();
            $this->start();

            // Throw an exception so that:
            // - this is reported as a Lambda execution error ("error rate" metrics are accurate)
            // - the CloudWatch logs correctly reflect that an execution error occurred
            // - the 500 response is the same as if an exception happened in Bref
            throw new Timeout($timeoutDelayInMs, $context->getAwsRequestId());
        } catch (Throwable $e) {
            printf(
                "Error communicating with PHP-FPM to read the HTTP response. Bref will restart PHP-FPM now. Original exception message: %s %s\n",
                get_class($e),
                $e->getMessage()
            );

            // Restart PHP-FPM: in some cases PHP-FPM is borked, that's the only way we can recover
            $this->stop();
            $this->start();

            throw new FastCgiCommunicationFailed;
        }

        $responseHeaders = $this->getResponseHeaders($response);

        // Extract the status code
        if (isset($responseHeaders['status'])) {
            $status = (int) (is_array($responseHeaders['status']) ? $responseHeaders['status'][0] : $responseHeaders['status']);
            unset($responseHeaders['status']);
        }

        $this->ensureStillRunning();

        return new HttpResponse($response->getBody(), $responseHeaders, $status ?? 200);
    }

    /**
     * @throws Exception If the PHP-FPM process is not running anymore.
     */
    private function ensureStillRunning(): void
    {
        // if this process is not the owner of the php-fpm process, just no-op
        if (!$this->fpm) {
            return;
        }

        if (! $this->fpm->isRunning()) {
            throw new Exception('PHP-FPM has stopped for an unknown reason');
        }
    }

    private function waitUntilReady(): void
    {
        $wait = 5000; // 5ms
        $timeout = 5000000; // 5 secs
        $elapsed = 0;

        while (! $this->isReady()) {
            usleep($wait);
            $elapsed += $wait;

            if ($elapsed > $timeout) {
                throw new Exception('Timeout while waiting for PHP-FPM socket at ' . self::SOCKET);
            }

            // If the process has crashed we can stop immediately
            if (! $this->fpm?->isRunning()) {
                throw new Exception('PHP-FPM failed to start: ' . PHP_EOL . $this->fpm->getOutput() . PHP_EOL . $this->fpm->getErrorOutput());
            }
        }
    }

    private function isReady(): bool
    {
        clearstatcache(false, self::SOCKET);

        return file_exists(self::SOCKET);
    }

    private function eventToFastCgiRequest(HttpRequestEvent $event, Context $context): ProvidesRequestData
    {
        $request = new FastCgiRequest($event->getMethod(), $this->handler, $event->getBody());
        $request->setRequestUri($event->getUri());
        $request->setRemoteAddress('127.0.0.1');
        $request->setRemotePort($event->getRemotePort());
        $request->setServerAddress('127.0.0.1');
        $request->setServerName($event->getServerName());
        $request->setServerProtocol($event->getProtocol());
        $request->setServerPort($event->getServerPort());
        $request->setCustomVar('PATH_INFO', $event->getPath());
        $request->setCustomVar('QUERY_STRING', $event->getQueryString());
        $request->setCustomVar('LAMBDA_INVOCATION_CONTEXT', json_encode($context));
        $request->setCustomVar('LAMBDA_REQUEST_CONTEXT', json_encode($event->getRequestContext()));

        $contentType = $event->getContentType();
        if ($contentType) {
            $request->setContentType($contentType);
        }
        foreach ($event->getHeaders() as $header => $values) {
            foreach ($values as $value) {
                $key = 'HTTP_' . strtoupper(str_replace('-', '_', $header));
                $request->setCustomVar($key, $value);
            }
        }

        return $request;
    }

    /**
     * Return an array of the response headers.
     */
    private function getResponseHeaders(ProvidesResponseData $response): array
    {
        return array_change_key_case($response->getHeaders(), CASE_LOWER);
    }
}
