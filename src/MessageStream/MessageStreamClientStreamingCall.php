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

use Hyperf\Grpc\StatusCode;
use Hyperf\GrpcClient\Exception\GrpcClientException;
use Hyperf\GrpcClient\GrpcClient;

class MessageStreamClientStreamingCall extends MessageStreamingCall
{
    private bool $received = false;

    /**
     * Reads the complete response stream. A client-streaming RPC returns one response message.
     */
    public function recv(float $timeout = GrpcClient::GRPC_DEFAULT_TIMEOUT): MessageStreamReadResult
    {
        if ($this->received) {
            throw new GrpcClientException('MessageStreamClientStreamingCall can only call recv once!', StatusCode::INTERNAL);
        }
        $this->received = true;

        $deadline = $this->createDeadline($timeout);
        $messages = [];

        do {
            $result = parent::recv($this->remainingTimeout($deadline));
            array_push($messages, ...$result->messages);
        } while (! $result->ended);

        if (count($messages) !== 1) {
            throw new GrpcClientException('MessageStreamClientStreamingCall must receive exactly one response message.', StatusCode::INTERNAL);
        }

        return new MessageStreamReadResult($messages, true, $result->response);
    }
}
