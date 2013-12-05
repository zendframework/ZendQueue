<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2013 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace ZendQueue\Adapter;

use ZendQueue\Exception;
use ZendQueue\Message;
use ZendQueue\Queue;

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;


/**
 * Class for using AMQP to talk to an AMQP compliant server
 *
 * @category   Zend
 * @package    ZendQueue\Queue
 * @subpackage Adapter
 */

class Amqp extends AbstractAdapter
{
    const DEFAULT_HOST  = 'localhost';
    const DEFAULT_PORT  = 5672;
    const DEFAULT_USER  = 'guest';
    const DEFAULT_PASS  = 'guest';
    const DEFAULT_VHOST = '/';
    const DEFAULT_DEBUG = false;

    /**
     * @var \PhpAmqpLib\Connection\AMQPConnection
     */
    private $conn = null;

    /**
     *
     * @var \PhpAmqpLib\Connection\AMQPChannel
     */
    private $ch = null;

    /**
     *
     * @var string
     */
    private $exchange = 'router';


    /**
     *
     * @var string
     */
    private $exchangeType = 'direct';

    /**
     *
     * @var string
     */
    private $consumerTag = 'consumer';

    /**
     * Constructor
     *
     * @param  array|\Traversable $options An array having configuration data
     * @param  \ZendQueue\Queue The \ZendQueue\Queue object that created this class
     */
    public function __construct($options, Queue $queue = null)
    {
        parent::__construct($options, $queue);
        $options = &$this->_options['driverOptions'];
        if (!array_key_exists($key = 'host', $options)) {
            $options[$key] = self::DEFAULT_HOST;
        }
        if (!array_key_exists($key = 'port', $options)) {
            $options[$key] = self::DEFAULT_PORT;
        }
        if (!array_key_exists($key = 'user', $options)) {
            $options[$key] = self::DEFAULT_USER;
        }
        if (!array_key_exists($key = 'pass', $options)) {
            $options[$key] = self::DEFAULT_PASS;
        }
        if (!array_key_exists($key = 'vhost', $options)) {
            $options[$key] = self::DEFAULT_VHOST;
        }
        if (!array_key_exists($key = 'debug', $options)) {
            $options[$key] = self::DEFAULT_DEBUG;
        }
        if (array_key_exists($key = 'exchange', $options)) {
            $this->exchange = $options[$key];
        }
        if (array_key_exists($key = 'exchangeType', $options)) {
            $this->exchangeType = $options[$key];
        }
        if (array_key_exists($key = 'consumerTag', $options)) {
            $this->consumerTag = $options[$key];
        }
        $this->initAdapter();
    }

    /**
     * Initialize AMQP adapter
     */
    protected function initAdapter()
    {
        $options = &$this->_options['driverOptions'];
        defined('AMQP_DEBUG') ? AMQP_DEBUG : $options['debug'];
        $this->conn = new AMQPConnection($options['host'], $options['port'], $options['user'], $options['pass'], $options['vhost']);
        $this->ch = $this->conn->channel();
    }

    /**
     * Close the socket explicitly when destructed
     *
     * @return void
     */
    public function __destruct()
    {
    }

    /**
     * Does a queue already exist?
     *
     * Use isSupported('isExists') to determine if an adapter can test for
     * queue existance.
     *
     * @param  string $name Queue name
     * @return boolean
    */
    public function isExists($name)
    {
        return in_array($name, $this->_queues);
    }

    /**
     * Create a new queue
     *
     * Visibility timeout is how long a message is left in the queue
     * "invisible" to other readers.  If the message is acknowleged (deleted)
     * before the timeout, then the message is deleted.  However, if the
     * timeout expires then the message will be made available to other queue
     * readers.
     *
     * @param  string  $name Queue name
     * @param  integer $timeout Default visibility timeout
     * @return boolean
    */
    public function create($name, $timeout=null)
    {
        try {
            $key = array_search($name, $this->_queues);
            if ($key !== false) {
                return false;
            }
            $queue_declare = $this->ch->queue_declare($name, $passive=false, $durable=true, $exclusive=false, $auto_delete=false);
            $this->ch->exchange_declare($this->exchange, $this->exchangeType, $passive=false, $durable=true, $auto_delete=false);
            $this->ch->queue_bind($name, $this->exchange);
            $this->_queues[] = $name;
            return true;
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage(), $e->getCode(), $e);
        }
        return false;
    }

    /**
     * Delete a queue and all of its messages
     *
     * Return false if the queue is not found, true if the queue exists.
     *
     * @param  string $name Queue name
     * @return boolean
    */
    public function delete($name)
    {
        $result = true;
        try {
            try {
                $this->ch->queue_unbind($name, $this->exchange);
                $this->ch->exchange_delete($this->exchange);
                $queue_delete = $this->ch->queue_delete($name, $if_unused=false, $if_empty=false, $nowait=true, $ticket=false);
            } catch (\PhpAmqpLib\Exception\AMQPProtocolChannelException $e) {
                $result = false;
            } catch (\ZendQueue\Exception\RuntimeException $e) {
                $result = false;
            }
            $key = array_search($name, $this->_queues);
            if ($key !== false) {
                unset($this->_queues[$key]);
            }
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage(), $e->getCode(), $e);
        }
        return $result;
    }

    /**
     * Get an array of all available queues
     *
     * Not all adapters support getQueues(); use isSupported('getQueues')
     * to determine if the adapter supports this feature.
     *
     * @return array
    */
    public function getQueues()
    {
        throw new Exception\UnsupportedMethodCallException(__FUNCTION__ . '() is not supported in this adapter');
    }

    /**
     * Return the approximate number of messages in the queue
     *
     * @param  \ZendQueue\Queue|null $queue
     * @return integer
    */
    public function count(Queue $queue = null)
    {
        if ($queue === null) {
            $queue = $this->_queue;
        }
        $this->ch->close();
        $this->conn->close();
        $this->initAdapter();
        list($name, $messageCount, $consumerCount) = $this->ch->queue_declare($queue->getName(), $passive=false, $durable=true, $exclusive=false, $auto_delete=false);
        return $messageCount;
    }

    /********************************************************************
     * Messsage management functions
    *********************************************************************/

    /**
     * Send a message to the queue
     *
     * @param  mixed $message Message to send to the active queue
     * @param  \ZendQueue\Queue|null $queue
     * @return \ZendQueue\Message
    */
    public function send($message, Queue $queue = null)
    {
        if ($queue === null) {
            $queue = $this->_queue;
        }
        if (is_scalar($message)) {
            $message = (string)$message;
        }
        if (is_string($message)) {
            $message = trim($message);
        }
        try {
            $amqpMessage = new AMQPMessage($message);
            $this->ch->basic_publish($amqpMessage, $this->exchange);
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage(), $e->getCode(), $e);
        }
        $data    = array(
            'message_id' => md5(uniqid(rand(), true)),
            'handle'     => md5(uniqid(rand(), true)),
            'body'       => $message,
            'md5'        => md5($message),
        );
        $options = array(
            'queue' => $queue,
            'data'  => $data,
        );
        $classname = $queue->getMessageClass();
        return new $classname($options);
    }

    /**
     * Get messages in the queue
     *
     * @param  integer|null $maxMessages Maximum number of messages to return
     * @param  integer|null $timeout Visibility timeout for these messages
     * @param  \ZendQueue\Queue|null $queue
     * @return \ZendQueue\Message\MessageIterator
    */
    public function receive($maxMessages = null, $timeout = null, Queue $queue = null)
    {
        if ($maxMessages === null) {
            $maxMessages = 1;
        }
        if ($queue === null) {
            $queue = $this->_queue;
        }
        $i = 0;
        $this->msgs = array();
        if ($maxMessages > 0 ) {
            while(($amqpMessage = $this->ch->basic_get($queue->getName())) && $i < $maxMessages) {
                $this->processMsgAck($amqpMessage);
                $i++;
            }
        }
        $options = array(
            'queue'        => $queue,
            'data'         => $this->msgs,
            'messageClass' => $queue->getMessageClass(),
        );
        $this->msgs = array();
        $classname = $queue->getMessageSetClass();
        return new $classname($options);
    }

    /**
     *
     * @param PhpAmqpLib\Message\AMQPMessage $amqpMessage
     */
    public function processMsgAck(\PhpAmqpLib\Message\AMQPMessage $amqpMessage)
    {
        $data = array(
            'handle' => md5(uniqid(rand(), true)),
            'body'   => (string)$amqpMessage->body,
        );
        $this->msgs[] = $data;
        /*
         * acknowledge one or more messages
         */
        if (array_key_exists('channel', $amqpMessage->delivery_info)) {
            $amqpMessage->delivery_info['channel']->basic_ack($amqpMessage->delivery_info['delivery_tag']);
            /*
             * Send a message with the string "quit" to cancel the consumer.
             */
            if ('quit' === $amqpMessage->body) {
                $amqpMessage->delivery_info['channel']->basic_cancel($amqpMessage->delivery_info['consumer_tag']);
            }
        } else {
            $this->ch->basic_ack($amqpMessage->delivery_info['delivery_tag']);
        }
    }

    /**
     *
     * @param PhpAmqpLib\Message\AMQPMessage $amqpMessage
     */
    public function processMsgNoneAck(\PhpAmqpLib\Message\AMQPMessage $amqpMessage)
    {
        /*
         * Send a message with the string "quit" to cancel the consumer.
        */
        if ('quit' === $amqpMessage->body) {
            $amqpMessage->delivery_info['channel']->basic_cancel($amqpMessage->delivery_info['consumer_tag']);
        }
        $this->count++;
    }

    /**
     * Delete a message from the queue
     *
     * Return true if the message is deleted, false if the deletion is
     * unsuccessful.
     *
     * @param  \ZendQueue\Message $message
     * @return boolean
    */
    public function deleteMessage(Message $message)
    {
        throw new Exception\UnsupportedMethodCallException(__FUNCTION__ . '() is not supported in this adapter');
    }

    /********************************************************************
     * Supporting functions
    *********************************************************************/

    /**
     * Returns the configuration options in this adapter.
     *
     * @return array
    */
    public function getOptions()
    {
        return parent::getOptions();
    }

    /**
     * Return a list of queue capabilities functions
     *
     * $array['function name'] = true or false
     * true is supported, false is not supported.
     *
     * @return array
    */
    public function getCapabilities()
    {
        return array(
            'create'        => true,
            'delete'        => true,
            'send'          => true,
            'receive'       => true,
            'deleteMessage' => false,
            'getQueues'     => false,
            'count'         => true,
            'isExists'      => true,
        );
    }
}
