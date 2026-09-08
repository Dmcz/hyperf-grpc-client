<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace Hyperf\GrpcClient\MessageStream;

use Swoole\Http2\Response;

final class MessageStreamReadResult
{
    public function __construct(
        public readonly array $messages,
        public readonly bool $ended,
        public readonly Response $response,
    ) {
    }
}
