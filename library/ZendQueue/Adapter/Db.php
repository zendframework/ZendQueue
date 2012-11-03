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

use Zend\Db\Sql\Sql;

use Zend\Db\Adapter\Driver\StatementInterface;

use Zend\Db\ResultSet\ResultSet;

use Zend\Db\TableGateway\Feature\RowGatewayFeature;

use Zend\Db\TableGateway\TableGateway;

use Traversable;
use Zend\Db as DB_ns;
use Zend\Db\Sql\Select;
use Zend\Db\Adapter\Adapter as AbstractDBAdapter;
use ZendQueue\Exception;
use ZendQueue\Message;
use ZendQueue\Queue;

/**
 * Class for using connecting to a Zend_DB-based queuing system
 *
 * @category   Zend
 * @package    Zend_Queue
 * @subpackage Adapter
 */
class Db extends AbstractAdapter
{
    /**
     * @var Db\Queue
     */
    protected $_queueTable = null;

    /**
     * @var Db\Message
     */
    protected $_messageTable = null;

    /**
     * @var \Zend\Db\Adapter\Adapter
     */
    protected $_adapter = null;
    
    /**
     * Constructor
     *
     * @param  array|Traversable $options
     * @param  Queue|null $queue
     */
    public function __construct($options, Queue $queue = null)
    {
        parent::__construct($options, $queue);

        $this->_adapter = $this->_initDBAdapter();
        $this->_queueTable = new \ZendQueue\Adapter\Db\Queue($this->_adapter);
        $this->_messageTable = new \ZendQueue\Adapter\Db\Message($this->_adapter);
        $this->create($queue->getName());

    }

    /**
     * Get the TableGateway implementation of the queue table
     * 
     * @return \Db\Queue
     */
    public function getQueueTable()
    {
        return $this->_queueTable;
    }
    
    /**
     * Get the TableGateway implementation of the message table
     *
     * @return \Db\Message
     */
    public function getMessageTable()
    {
        return $this->_messageTable;
    }

    /**
     * Initialize DB adapter using 'driverOptions' section of the _options array
     *
     * Throws an exception if the adapter cannot connect to DB.
     *
     * @return \Zend\Db\Adapter\AbstractAdapter
     * @throws Exception\InvalidArgumentException
     * @throws Exception\ConnectionException
     */
    protected function _initDBAdapter()
    {
        $options = &$this->_options['driverOptions'];
        if (!array_key_exists('type', $options)) {
            throw new Exception\InvalidArgumentException("Configuration array must have a key for 'type' for the database type to use");
        }

        if (!array_key_exists('host', $options)) {
            throw new Exception\InvalidArgumentException("Configuration array must have a key for 'host' for the host to use");
        }

        if (!array_key_exists('username', $options)) {
            throw new Exception\InvalidArgumentException("Configuration array must have a key for 'username' for the username to use");
        }

        if (!array_key_exists('password', $options)) {
            throw new Exception\InvalidArgumentException("Configuration array must have a key for 'password' for the password to use");
        }

        if (!array_key_exists('dbname', $options)) {
            throw new Exception\InvalidArgumentException("Configuration array must have a key for 'dbname' for the database to use");
        }

        $type = $options['type'];
        unset($options['type']);

        try {
            $db = new DB_ns\Adapter\Adapter(array_merge($options, array('driver' => $type)));
        } catch (DB_ns\Exception\ExceptionInterface $e) {
            throw new Exception\ConnectionException('Error connecting to database: ' . $e->getMessage(), $e->getCode(), $e);
        }

        return $db;
    }

    /********************************************************************
     * Queue management functions
     *********************************************************************/

    /**
     * Does a queue already exist?
     *
     * Throws an exception if the adapter cannot determine if a queue exists.
     * use isSupported('isExists') to determine if an adapter can test for
     * queue existance.
     *
     * @param  string $name
     * @return boolean
     * @throws Exception\ExceptionInterface
     */
    public function isExists($name)
    {
        $id = 0;

        try {
            $id = $this->getQueueId($name);
        } catch (\Exception $e) {
            return false;
        }

        return ($id > 0);
    }

    /**
     * Create a new queue
     *
     * Visibility timeout is how long a message is left in the queue "invisible"
     * to other readers.  If the message is acknowleged (deleted) before the
     * timeout, then the message is deleted.  However, if the timeout expires
     * then the message will be made available to other queue readers.
     *
     * @param  string  $name    queue name
     * @param  integer $timeout default visibility timeout
     * @return boolean
     * @throws Exception\RuntimeException - database error
     */
    public function create($name, $timeout = null)
    {
        if ($this->isExists($name)) {
            return false;
        }

        try { 
            $result = $this->_queueTable->insert(array('queue_name' => $name, 'timeout' => 30));
        } catch(\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage(), $e->getCode());
        }

        if ($result) {
            return true;
        }

        return false;
    }

    /**
     * Delete a queue and all of it's messages
     *
     * Returns false if the queue is not found, true if the queue exists
     *
     * @param  string  $name queue name
     * @return boolean
     * @throws Exception\RuntimeException - database error
     */
    public function delete($name)
    {
        $id = $this->getQueueId($name); // get primary key

        // if the queue does not exist then it must already be deleted.
        $list = $this->_queueTable->select(array('queue_id' => $id));
        if (count($list) === 0) {
            return false;
        }

        try {
            $remove = $this->_queueTable->delete(array('queue_id' => $id));
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage(), $e->getCode(), $e);
        }

        if (array_key_exists($name, $this->_queues)) {
            unset($this->_queues[$name]);
        }

        return true;
    }

    /**
     * Get an array of all available queues
     *
     * Not all adapters support getQueues(), use isSupported('getQueues')
     * to determine if the adapter supports this feature.
     *
     * @return array
     * @throws Exception\ExceptionInterface - database error
     */
    public function getQueues()
    {
        $result = $this->_queueTable->select();
        foreach($result as $one) {
            $this->_queues[$one['queue_name']] = (int)$one['queue_id'];
        }
        
        $list = array_keys($this->_queues);

        return $list;
    }

    /**
     * Return the approximate number of messages in the queue
     *
     * @param  Queue $queue
     * @return integer
     * @throws Exception\ExceptionInterface
     */
    public function count(Queue $queue = null)
    {
        if ($queue === null) {
            $queue = $this->_queue;
        }

        $info  = $this->_messageTable->select(array('queue_id' => $this->getQueueId($queue->getName())));

        // return count results
        return $info->count();
    }

    /********************************************************************
    * Messsage management functions
     *********************************************************************/

    /**
     * Send a message to the queue
     *
     * @param  string     $message Message to send to the active queue
     * @param  Queue $queue
     * @return Message
     * @throws Exception\QueueNotFoundException
     * @throws Exception\RuntimeException
     */
    public function send($message, Queue $queue = null)
    {
        if ($queue === null) {
            $queue = $this->_queue;
        }

        if (is_scalar($message)) {
            $message = (string) $message;
        }
        if (is_string($message)) {
            $message = trim($message);
        }

        if (!$this->isExists($queue->getName())) {
            throw new Exception\QueueNotFoundException('Queue does not exist:' . $queue->getName());
        }

        $msg = array(
            'queue_id' => $this->getQueueId($queue->getName()),
            'created'  => time(),
            'body' => $message,
            'md5' => md5($message)
        );
        // $msg->timeout = ??? @TODO

        try {
            $this->_messageTable->insert($msg);
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage(), $e->getCode(), $e);
        }

        $options = array(
            'queue' => $queue,
            'data'  => $msg,
        );
        $classname = $queue->getMessageClass();
        return new $classname($options);
    }

    /**
     * Get messages in the queue
     *
     * @param  integer    $maxMessages  Maximum number of messages to return
     * @param  integer    $timeout      Visibility timeout for these messages
     * @param  Queue $queue
     * @return Message\MessageIterator
     * @throws Exception\RuntimeException - database error
     */
    public function receive($maxMessages = null, $timeout = null, Queue $queue = null)
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

        $msgs      = array();
        $name      = $this->_messageTable->table;
        $microtime = microtime(true); // cache microtime
        $db        = $this->_messageTable->getAdapter();
        $connection = $db->getDriver()->getConnection();

        // start transaction handling
        try {
            if ($maxMessages > 0 ) { // ZF-7666 LIMIT 0 clause not included.
                $connection->beginTransaction();

                $sql = new Sql($this->_adapter);
                $select = $sql->select();
                $select->from($name);
                $select->where(array('queue_id' => $this->getQueueId($queue->getName())));
                $select->where('(handle IS NULL OR timeout+' . (int)$timeout . ' < ' . (int)$microtime.')');
                $select->limit($maxMessages);
                 
                $statement = $sql->prepareStatementForSqlObject($select);
                $result = $statement->execute();
                
                foreach($result as $message) {
                    
                    $update = $sql->update('message');
                    $update->where(array('message_id' => $message['message_id']));
                    $update->where('(handle IS NULL OR timeout+' . (int)$timeout . ' < ' . (int)$microtime.')');
                    $message['handle'] = md5(uniqid(rand(), true));
                    $message['timeout'] = $microtime;
                    $update->set(array('handle' => $message['handle'], 'timeout' => $microtime));
                    $stmt = $sql->prepareStatementForSqlObject($update);
                    $rst = $stmt->execute();
                    if ($rst->count() > 0) {
                        $msgs[] = $message;
                    }
                    
                }
                $connection->commit();
            }
        } catch (\Exception $e) {
            $connection->rollBack();
            throw $e;
        }

        $options = array(
            'queue'        => $queue,
            'data'         => $msgs,
            'messageClass' => $queue->getMessageClass(),
        );
        $classname = $queue->getMessageSetClass();
        return new $classname($options);
        
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
        $data = $message->toArray();
        $db    = $this->_messageTable->delete(array('handle' => $data['handle']));

        if ($db) {
            return true;
        }
        return false;
    }

    /********************************************************************
     * Supporting functions
     *********************************************************************/

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
            'create'        => true,
            'delete'        => true,
            'send'          => true,
            'receive'       => true,
            'deleteMessage' => true,
            'getQueues'     => true,
            'count'         => true,
            'isExists'      => true,
        );
    }

    /********************************************************************
     * Functions that are not part of the \ZendQueue\Adapter\AdapterAbstract
     *********************************************************************/
    /**
     * Get the queue ID
     *
     * Returns the queue's row identifier.
     *
     * @param  string       $name
     * @return integer|null
     * @throws Exception\QueueNotFoundException
     */
    protected function getQueueId($name)
    {
        if (array_key_exists($name, $this->_queues)) {
            return $this->_queues[$name];
        }

        $result = $this->_queueTable->select(array('queue_name' => $name));
        foreach($result as $one) {
            $this->_queues[$name] = (int)$one['queue_id'];
        }
        
        if (!array_key_exists($name, $this->_queues)) {
            throw new Exception\QueueNotFoundException('Queue does not exist: ' . $name);
        }

        return $this->_queues[$name];
    }
}
