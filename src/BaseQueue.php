<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace adamyxt\rabbitmq;

use yii\base\Component;
use yii\di\Instance;
use adamyxt\rabbitmq\serializers\PhpSerializer;
use adamyxt\rabbitmq\serializers\SerializerInterface;

/**
 * Base Queue
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
abstract class BaseQueue extends Component
{

    /**
     * @var SerializerInterface|array
     */
    public $serializer = PhpSerializer::class;

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        $this->serializer = Instance::ensure($this->serializer, SerializerInterface::class);
    }
}