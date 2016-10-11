<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Queue
 */

namespace ZendQueue\Adapter;

use Traversable;
use ZendQueue\Exception;
use ZendQueue\Message;
use ZendQueue\Queue;
use ZendQueue\Stomp\Client;

/**
 * Class for using Stomp to talk to an Stomp compliant server
 *
 * @category   Zend
 * @package    Zend_Queue
 * @subpackage Adapter
 */
class Activemq extends AbstractAdapter
{
    const DEFAULT_SCHEME = 'tcp';
    const DEFAULT_HOST   = '127.0.0.1';
    const DEFAULT_PORT   = 61613;

    /**
     * @var \ZendQueue\Stomp\Client
     */
    private $_client = null;

    /**
     * @var array
     */
    private $_subscribed = array();

    /**
     * Constructor
     *
     * @param  array|Traversable $options An array having configuration data
     * @param  Queue The Queue object that created this class
     * @throws Exception\ConnectionException
     */
    public function __construct($options, Queue $queue = null)
    {
        parent::__construct($options);

        $options = &$this->_options['driverOptions'];
        if (!array_key_exists('scheme', $options)) {
            $options['scheme'] = self::DEFAULT_SCHEME;
        }
        if (!array_key_exists('host', $options)) {
            $options['host'] = self::DEFAULT_HOST;
        }
        if (!array_key_exists('port', $options)) {
            $options['port'] = self::DEFAULT_PORT;
        }

        if (array_key_exists('stompClient', $options)) {
            $this->_client = $options['stompClient'];
        } else {
            $this->_client = new Client($options['scheme'], $options['host'], $options['port']);
        }

        $connect = $this->_client->createFrame();

        // Username and password are optional on some messaging servers
        // such as Apache's ActiveMQ
        $connect->setCommand('CONNECT');
        if (isset($options['username'])) {
            $connect->setHeader('login', $options['username']);
            $connect->setHeader('passcode', $options['password']);
        }

        $response = $this->_client->send($connect)->receive();

        if ((false !== $response)
            && ($response->getCommand() != 'CONNECTED')
        ) {
            throw new Exception\ConnectionException(
                "Unable to authenticate to '{$options['scheme']}://{$options['host']}:{$options['port']}'"
            );
        }
    }

    /**
     * Close the socket explicitly when destructed
     *
     * @return void
     */
    public function __destruct()
    {
        // Gracefully disconnect
        $frame = $this->_client->createFrame();
        $frame->setCommand('DISCONNECT');
        $this->_client->send($frame);
        unset($this->_client);
    }

    /**
     * Create a new queue
     *
     * @param  string  $name    queue name
     * @param  integer $timeout default visibility timeout
     * @return void
     * @throws Exception\UnsupportedMethodCallException
     */
    public function create($name, $timeout=null)
    {
        throw new Exception\UnsupportedMethodCallException('create() is not supported in ' . get_called_class());
    }

    /**
     * Delete a queue and all of its messages
     *
     * @param  string $name queue name
     * @return void
     * @throws Exception\UnsupportedMethodCallException
     */
    public function delete($name)
    {
        throw new Exception\UnsupportedMethodCallException('delete() is not supported in ' . get_called_class());
    }

    /**
     * Delete a message from the queue
     *
     * Returns true if the message is deleted, false if the deletion is
     * unsuccessful.
     *
     * @param  Message $message
     * @return boolean
     */
    public function deleteMessage(Message $message)
    {
        $frame = $this->_client->createFrame();
        $frame->setCommand('ACK');
        $frame->setHeader('message-id', $message->handle);

        $this->_client->send($frame);

        return true;
    }

    /**
     * Get an array of all available queues
     *
     * @return void
     * @throws Exception\UnsupportedMethodCallException
     */
    public function getQueues()
    {
        throw new Exception\UnsupportedMethodCallException('getQueues() is not supported in this adapter');
    }

    /**
     * Checks if the client is subscribed to the queue
     *
     * @param  Queue $queue
     * @return boolean
     */
    protected function isSubscribed(Queue $queue)
    {
        return isset($this->_subscribed[$queue->getName()]);
    }

    /**
     * Subscribes the client to the queue.
     *
     * @param  Queue $queue
     * @return void
     */
    protected function subscribe(Queue $queue)
    {
        $frame = $this->_client->createFrame();
        $frame->setCommand('SUBSCRIBE');
        $frame->setHeader('destination', $queue->getName());
        $frame->setHeader('ack','client');
        $this->_client->send($frame);
        $this->_subscribed[$queue->getName()] = TRUE;
    }

    /**
     * Return the first element in the queue
     *
     * @param  integer    $maxMessages
     * @param  integer    $timeout
     * @param  \ZendQueue\Queue $queue
     * @return \ZendQueue\Message\MessageIterator
     * @throws Exception\UnexpectedValueException
     */
    public function receive($maxMessages=null, $timeout=null, Queue $queue=null)
    {
        if ($maxMessages === null) {
            $maxMessages = 1;
        }
        if ($timeout === null) {
            $timeout = self::RECEIVE_TIMEOUT_DEFAULT;
        }
        if ($queue === null) {
            $queue = $this->_queue;
        }

        // read
        $data = array();

        // signal that we are reading
        if(!$this->isSubscribed($queue)) {
            $this->subscribe($queue);
        }

        if ($maxMessages > 0) {
            if ($this->_client->canRead()) {
                for ($i = 0; $i < $maxMessages; $i++) {
                    $response = $this->_client->receive();

                    switch ($response->getCommand()) {
                        case 'MESSAGE':
                            $datum = array(
                                'message_id' => $response->getHeader('message-id'),
                                'handle'     => $response->getHeader('message-id'),
                                'body'       => $response->getBody(),
                                'md5'        => md5($response->getBody())
                            );
                            $data[] = $datum;
                            break;
                        default:
                            $block = print_r($response, true);
                            throw new Exception\UnexpectedValueException('Invalid response received: ' . $block);
                    }
                }
            }
        }

        $options = array(
            'queue'        => $queue,
            'data'         => $data,
            'messageClass' => $queue->getMessageClass(),
        );
        $classname = $queue->getMessageSetClass();
        return new $classname($options);
    }

    /**
     * Push an element onto the end of the queue
     *
     * @param  string     $message message to send to the queue
     * @param  Queue $queue
     * @return Message
     */
    public function send($message, Queue $queue=null)
    {
        if ($queue === null) {
            $queue = $this->_queue;
        }

        $frame = $this->_client->createFrame();
        $frame->setCommand('SEND');
        $frame->setHeader('destination', $queue->getName());
        $frame->setHeader('content-length', strlen($message));
        $frame->setBody((string) $message);
        $this->_client->send($frame);

        $data = array(
            'message_id' => null,
            'body'       => $message,
            'md5'        => md5($message),
            'handle'     => null
        );

        $options = array(
            'queue' => $queue,
            'data'  => $data
        );
        $classname = $queue->getMessageClass();
        return new $classname($options);
    }

    /**
     * Returns the length of the queue
     *
     * @param  ueue $queue
     * @return integer
     * @throws Exception\UnsupportedMethodCallException (not supported)
     */
    public function count(Queue $queue=null)
    {
        throw new Exception\UnsupportedMethodCallException('count() is not supported in this adapter');
    }

    /**
     * Does a queue already exist?
     *
     * @param  string $name
     * @return boolean
     * @throws Exception\UnsupportedMethodCallException (not supported)
     */
    public function isExists($name)
    {
        throw new Exception\UnsupportedMethodCallException('isExists() is not supported in this adapter');
    }

    /**
     * Return a list of queue capabilities functions
     *
     * $array['function name'] = true or false
     * true is supported, false is not supported.
     *
     * @param  string $name
     * @return array
     */
    public function getCapabilities()
    {
        return array(
            'create'        => false,
            'delete'        => false,
            'send'          => true,
            'receive'       => true,
            'deleteMessage' => true,
            'getQueues'     => false,
            'count'         => false,
            'isExists'      => false,
        );
    }
}
