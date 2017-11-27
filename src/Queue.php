<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace adamyxt\rabbitmq;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Yii;
use yii\base\Application as BaseApp;
use yii\base\Event;
use yii\base\InvalidConfigException;
use yii\caching\CacheInterface;

/**
 * rabbit Queue
 *
 */
class Queue extends CliQueue
{
    public $clusters = [];
    public $queueName = 'queue';
    public $exchangeName = 'exchange';
    /**
     * @var CacheInterface|string the cache object or the ID of the cache application component that is used to store
     * the health status of the DB servers specified in [[masters]] and [[slaves]].
     * This is used only when read/write splitting is enabled or [[masters]] is not empty.
     */
    public $serverStatusCache = 'cache';
    /**
     * @var int the retry interval in seconds for dead servers listed in [[masters]] and [[slaves]].
     * This is used together with [[serverStatusCache]].
     */
    public $serverRetryInterval = 10;
    /**
     * @var string command class name
     */
    public $commandClass = Command::class;

    /**
     * @var AMQPStreamConnection
     */
    protected $connection;
    /**
     * @var AMQPChannel
     */
    protected $channel;


    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        Event::on(BaseApp::class, BaseApp::EVENT_AFTER_REQUEST, function () {
            $this->close();
        });
    }

    /**
     * Listens amqp-queue and runs new jobs.
     */
    public function listen()
    {
        $this->open();
        $callback = function(AMQPMessage $payload) {
            list($ttr, $message) = explode(';', $payload->body, 2);
            echo 123;
                $payload->delivery_info['channel']->basic_ack($payload->delivery_info['delivery_tag']);
        };
        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume($this->queueName, '', false, false, false, false, $callback);
        while(count($this->channel->callbacks)) {
            $this->channel->wait();
        }
    }

    /**
     * @inheritdoc
     */
    protected function pushMessage($message, $ttr)
    {
        $this->open();
        $this->channel->basic_publish(
            new AMQPMessage("$ttr;$message", [
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            ]),
            $this->exchangeName
        );

        return null;
    }

    /**
     * Opens connection and channel
     */
    protected function open()
    {
        if ($this->channel) return;
        $this->connection = $this->openFromPool($this->clusters);
        if (!$this->connection) {
            throw new InvalidConfigException('There is no available queue service.');
        }
        $this->channel = $this->connection->channel();
        $this->channel->queue_declare($this->queueName, false, true, false, false);
        $this->channel->exchange_declare($this->exchangeName, 'direct', false, true, false);
        $this->channel->queue_bind($this->queueName, $this->exchangeName);
    }

    /**
     * Closes connection and channel
     */
    protected function close()
    {
        if (!$this->channel) return;
        $this->channel->close();
        $this->connection->close();
    }

    /**
     */
    protected function openFromPool(array $pool)
    {
        shuffle($pool);
        return $this->openFromPoolSequentially($pool);
    }

    /**
     */
    protected function openFromPoolSequentially(array $pool)
    {
        if (empty($pool)) {
            return null;
        }

        $cache = is_string($this->serverStatusCache) ? Yii::$app->get($this->serverStatusCache, false) : $this->serverStatusCache;

        foreach ($pool as $config) {

            $key = [__METHOD__, $config['host']];
            if ($cache instanceof CacheInterface && $cache->get($key)) {
                // should not try this dead server now
                continue;
            }

            try {
                $connection = new AMQPStreamConnection($this->clusters['host']??'localhost', $this->clusters['port']??'5672', $this->clusters['user']??'guest', $this->clusters['password']??'guest', $this->clusters['vhost']??'/');
                return $connection;
            } catch (\Exception $e) {
                Yii::warning("Connection ({$config['host']}) failed: " . $e->getMessage(), __METHOD__);
                if ($cache instanceof CacheInterface) {
                    // mark this server as dead and only retry it after the specified interval
                    $cache->set($key, 1, $this->serverRetryInterval);
                }
            }
        }

        return null;
    }
}
