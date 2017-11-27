<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace adamyxt\rabbitmq\serializers;


/**
 * Interface SerializerInterface
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
interface SerializerInterface
{
    /**
     * @return string
     */
    public function serialize($job);

    /**
     * @param string $serialized
     */
    public function unserialize($serialized);
}