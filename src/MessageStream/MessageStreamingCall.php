<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace Hyperf\GrpcClient\MessageStream;

use Hyperf\Grpc\Parser;
use Hyperf\Grpc\StatusCode;
use Hyperf\Grpc\StreamMessageParser;
use Hyperf\GrpcClient\Exception\GrpcClientException;
use Hyperf\GrpcClient\GrpcClient;
use RuntimeException;
use Swoole\Http2\Response;

class MessageStreamingCall
{
    protected GrpcClient $client;

    protected string $method = '';

    protected mixed $deserialize = null;

    protected int $streamId = 0;

    protected array $metadata = [];

    private ?StreamMessageParser $parser = null;

    public function setClient(GrpcClient $client): self
    {
        $this->client = $client;

        return $this;
    }

    public function setMethod(string $method): self
    {
        $this->method = $method;

        return $this;
    }

    public function setDeserialize(mixed $deserialize): self
    {
        $this->deserialize = $deserialize;

        return $this;
    }

    public function getStreamId(): int
    {
        return $this->streamId;
    }

    public function setStreamId(int $streamId): self
    {
        if ($this->streamId !== $streamId) {
            $this->parser = null;
        }

        $this->streamId = $streamId;

        return $this;
    }

    public function setMetadata(array $metadata): self
    {
        $this->metadata = $metadata;

        return $this;
    }

    public function send($message = null): void
    {
        if ($this->getStreamId() > 0) {
            throw new RuntimeException('You can only send a streaming call once unless retrying after the connection is closed.');
        }

        $streamId = $this->client->openStream(
            $this->method,
            Parser::serializeMessage($message),
            '',
            true,
            $this->metadata
        );
        if ($streamId <= 0) {
            throw $this->newException();
        }
        $this->setStreamId($streamId);
    }

    public function push($message): void
    {
        if ($this->getStreamId() <= 0) {
            $this->setStreamId($this->client->openStream(
                $this->method,
                null,
                '',
                true,
                $this->metadata
            ));
        }

        $success = $this->client->write(
            $this->getStreamId(),
            Parser::serializeMessage($message),
            false
        );
        if (! $success) {
            throw $this->newException();
        }
    }

    /**
     * Reads until at least one complete message is available or the stream ends.
     * The timeout applies to the complete read operation, including partial data.
     */
    public function recv(float $timeout = -1.0): MessageStreamReadResult
    {
        $deadline = $this->createDeadline($timeout);

        while (true) {
            $streamId = $this->getStreamId();
            if ($streamId <= 0) {
                throw $this->newException();
            }

            $recv = $this->client->recv($streamId, $this->remainingTimeout($deadline));
            if (! $recv instanceof Response) {
                $this->setStreamId(0);
                throw $this->newException();
            }

            $ended = $recv->pipeline === false;
            if (! $ended && ! $this->client->isStreamExist($streamId)) {
                $this->setStreamId(0);
                throw $this->newException();
            }

            try {
                $this->parser ??= new StreamMessageParser(
                    $this->deserialize,
                    $recv->headers['grpc-encoding'] ?? 'identity'
                );

                $messages = $this->parser->parseChunk($recv->data ?? '');

                if ($ended) {
                    $this->assertGrpcStatusOk($recv);
                    $this->parser->finish();
                }
            } finally {
                if ($ended) {
                    $this->setStreamId(0);
                }
            }

            if ($messages !== [] || $ended) {
                return new MessageStreamReadResult($messages, $ended, $recv);
            }
        }
    }

    protected function createDeadline(float $timeout): ?float
    {
        return $timeout < 0 ? null : microtime(true) + $timeout;
    }

    protected function remainingTimeout(?float $deadline): float
    {
        if (is_null($deadline)) {
            return -1.0;
        }

        // Let GrpcClient::recv() perform timeout cleanup for the stream channel.
        return max(0.000001, $deadline - microtime(true));
    }

    private function assertGrpcStatusOk(Response $response): void
    {
        if (! isset($response->headers['grpc-status'])) {
            throw new GrpcClientException('gRPC stream ended without grpc-status.', StatusCode::UNKNOWN);
        }

        $status = (int) $response->headers['grpc-status'];
        if ($status !== StatusCode::OK) {
            throw new GrpcClientException(
                rawurldecode($response->headers['grpc-message'] ?? 'Unknown error'),
                $status
            );
        }
    }

    public function end(): void
    {
        if ($this->getStreamId() <= 0) {
            throw $this->newException();
        }

        if (! $this->client->write($this->getStreamId(), null, true)) {
            throw $this->newException();
        }
    }

    private function newException(): GrpcClientException
    {
        return new GrpcClientException(
            'the remote server may have been disconnected or timed out',
            StatusCode::INTERNAL
        );
    }
}
