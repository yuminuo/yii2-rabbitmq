<?php
/**
 *rabbitmq queue
 */

namespace adamyxt\rabbitmq;

use Yii;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
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
    private $callback_queue;
    private $response;
    private $corr_id;

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
        $this->open($this->exchangeName, $this->queueName);
        $callback = function(AMQPMessage $payload) {
            list($mode, $message) = explode(';', $payload->body, 2);
            $red = $this->handleMessage($message);
            if ($mode==2) {
                $msg = new AMQPMessage(
                    $this->serializer->serialize($red),
                    ['correlation_id' => $payload->get('correlation_id')]
                );
                $payload->delivery_info['channel']->basic_publish($msg, '', $payload->get('reply_to'));
            }
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
    public function push($exchangeName, $queueName, $message)
    {
        if ($exchangeName==null || $queueName==null) {
            throw new InvalidConfigException('exchangeName and queueName can not be empty.');
        }

        $this->open($exchangeName, $queueName);
        $message = $this->serializer->serialize($message);
        $this->channel->basic_publish(
            new AMQPMessage("1;$message", [
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            ]),
            $exchangeName
        );

        return null;
    }

    /**
     * @inheritdoc
     */
    public function synchPush($exchangeName, $queueName, $message)
    {
        if ($exchangeName==null || $queueName==null) {
            throw new InvalidConfigException('exchangeName and queueName can not be empty.');
        }

        $this->open($exchangeName, $queueName);
        list($this->callback_queue, ,) = $this->channel->queue_declare("", false, false, true, false);
        $this->channel->basic_consume($this->callback_queue, '', false, false, false, false, [$this, 'on_response']);
        $message = $this->serializer->serialize($message);

        $this->response = null;
        $this->corr_id = uniqid();

        $this->channel->basic_publish(
            new AMQPMessage("2;$message", [
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
                'correlation_id' => $this->corr_id,
                'reply_to' => $this->callback_queue
            ]),
            $exchangeName
        );
        while(!$this->response) {
            $this->channel->wait();
        }
        return $this->response;
    }

    /**
     * @param $rep
     */
    public function on_response($rep) {
        if($rep->get('correlation_id') == $this->corr_id) {
            $this->response = $this->serializer->unserialize($rep->body);
        }
    }

    /**
     * @inheritdoc
     */
    protected function handleMessage($message)
    {
        $data = $this->serializer->unserialize($message);
        if (!isset($data['workClass']) || !isset($data['method'])) {
            return ['status'=>false, 'msg'=>'parameter error.'];
        }
        try {
            $workModel = new $data['workClass'];
            $methodName = $data['method'];
            if (isset($data['data'])) {
                $workModel->$methodName($data['data']);
            } else {
                $workModel->$methodName();
            }
        }catch (\Exception $e) {
            return ['status'=>false, 'msg'=>$e->getMessage()];
        }

        return ['status'=>true];
    }

    /**
     * Opens connection and channel
     */
    protected function open($exchangeName, $queueName)
    {
        if ($this->channel) return;
        $this->connection = $this->openFromPool($this->clusters);
        if (!$this->connection) {
            throw new InvalidConfigException('There is no available queue service.');
        }
        $this->channel = $this->connection->channel();
        $this->channel->queue_declare($queueName, false, true, false, false);
        $this->channel->exchange_declare($exchangeName, 'direct', false, true, false);
        $this->channel->queue_bind($queueName, $exchangeName);
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
     * @param array $pool
     * @return null|AMQPStreamConnection
     */
    protected function openFromPool(array $pool)
    {
        shuffle($pool);
        return $this->openFromPoolSequentially($pool);
    }

    /**
     * @param array $pool
     * @return null|AMQPStreamConnection
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
                $connection = new AMQPStreamConnection($config['host']??'localhost', $config['port']??'5672', $config['user']??'guest', $config['password']??'guest', $config['vhost']??'/');
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
