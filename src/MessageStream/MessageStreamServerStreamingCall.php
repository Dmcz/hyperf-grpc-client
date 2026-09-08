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

class MessageStreamServerStreamingCall extends MessageStreamingCall
{
    public function push($message): void
    {
        throw new GrpcClientException('MessageStreamServerStreamingCall can not push data from client', StatusCode::INTERNAL);
    }
}
