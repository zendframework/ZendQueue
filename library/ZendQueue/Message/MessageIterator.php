<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Queue
 */

namespace ZendQueue\Message;

use Countable;
use Iterator;
use ZendQueue\Adapter\AdapterInterface;
use ZendQueue\Exception;
use ZendQueue\Queue;

/**
 * @category   Zend
 * @package    Zend_Queue
 * @subpackage Message
 */
class MessageIterator implements Countable, Iterator
{
    /**
     * The data for the queue message
     *
     * @var array
     */
    protected $_data = array();

     /**
     * Connected is true if we have a reference to a live
     * \ZendQueue\Adapter object.
     * This is false after the Message has been deserialized.
     *
     * @var boolean
     */
    protected $_connected = true;

    /**
     * Adapter parent class or instance
     *
     * @var AdapterInterface
     */
    protected $_queue = null;

    /**
     * Name of the class of the Adapter object.
     *
     * @var string
     */
    protected $_queueClass = null;

    /**
     * Message class name
     *
     * @var string
     */
    protected $_messageClass = '\ZendQueue\Message';

     /**
     * MessageIterator pointer.
     *
     * @var integer
     */
    protected $_pointer = 0;

    /**
     * Constructor
     *
     * @param  array $options ('queue', 'messageClass', 'data'=>array());
     * @throws Exception\InvalidArgumentException
     */
    public function __construct(array $options = array())
    {
        if (isset($options['queue'])) {
            $this->_queue      = $options['queue'];
            $this->_queueClass = get_class($this->_queue);
            $this->_connected  = true;
        } else {
            $this->_connected = false;
        }
        if (isset($options['messageClass'])) {
            $this->_messageClass = $options['messageClass'];
        }

        if (!is_array($options['data'])) {
            throw new Exception\InvalidArgumentException('array options must have $options[\'data\'] = array');
        }

        // set the message class
        $classname = $this->_messageClass;

        // for each of the messages
        foreach ($options['data'] as $data) {
            // construct the message parameters
            $message = array('data' => $data);

            // If queue has not been set, then use the default.
            if (empty($message['queue'])) {
                $message['queue'] = $this->_queue;
            }

            // construct the message and add it to _data[];
            $this->_data[] = new $classname($message);
        }
    }

    /**
     * Store queue and data in serialized object
     *
     * @return array
     */
    public function __sleep()
    {
        return array('_data', '_queueClass', '_messageClass', '_pointer');
    }

    /**
     * Setup to do on wakeup.
     * A de-serialized Message should not be assumed to have access to a live
     * queue connection, so set _connected = false.
     *
     * @return void
     */
    public function __wakeup()
    {
        $this->_connected = false;
    }

    /**
     * Returns all data as an array.
     *
     * Used for debugging.
     *
     * @return array
     */
    public function toArray()
    {
        // @todo This works only if we have iterated through
        // the result set once to instantiate the messages.
        foreach ($this->_data as $i => $message) {
            $this->_data[$i] = $message->toArray();
        }
        return $this->_data;
    }

    /**
     * Returns the queue object, or null if this is disconnected message set
     *
     * @return Queue|null
     */
    public function getQueue()
    {
        return $this->_queue;
    }

    /**
     * Set the queue object, to re-establish a live connection
     * to the queue for a Message that has been de-serialized.
     *
     * @param  AdapterInterface $queue
     * @return boolean
     * @throws Exception\ExceptionInterface
     */
    public function setQueue(Queue $queue)
    {
        $this->_queue     = $queue;
        $this->_connected = false;

        // @todo This works only if we have iterated through
        // the result set once to instantiate the rows.
        foreach ($this->_data as $i => $message) {
            $this->_connected = $this->_connected || $message->setQueue($queue);
        }

        return $this->_connected;
    }

    /**
     * Query the class name of the Queue object for which this
     * Message was created.
     *
     * @return string
     */
    public function getQueueClass()
    {
        return $this->_queueClass;
    }

    /*
     * MessageIterator implementation
     */

    /**
     * Rewind the MessageIterator to the first element.
     * Similar to the reset() function for arrays in PHP.
     * Required by interface MessageIterator.
     *
     * @return void
     */
    public function rewind()
    {
        $this->_pointer = 0;
    }

    /**
     * Return the current element.
     * Similar to the current() function for arrays in PHP
     * Required by interface MessageIterator.
     *
     * @return \ZendQueue\Message current element from the collection
     */
    public function current()
    {
        return (($this->valid() === false)
            ? null
            : $this->_data[$this->_pointer]); // return the messages object
    }

    /**
     * Return the identifying key of the current element.
     * Similar to the key() function for arrays in PHP.
     * Required by interface MessageIterator.
     *
     * @return integer
     */
    public function key()
    {
        return $this->_pointer;
    }

    /**
     * Move forward to next element.
     * Similar to the next() function for arrays in PHP.
     * Required by interface MessageIterator.
     *
     * @return void
     */
    public function next()
    {
        ++$this->_pointer;
    }

    /**
     * Check if there is a current element after calls to rewind() or next().
     * Used to check if we've iterated to the end of the collection.
     * Required by interface MessageIterator.
     *
     * @return bool False if there's nothing more to iterate over
     */
    public function valid()
    {
        return $this->_pointer < count($this);
    }

    /*
     * Countable Implementation
     */

    /**
     * Returns the number of elements in the collection.
     *
     * Implements Countable::count()
     *
     * @return integer
     */
    public function count()
    {
        return count($this->_data);
    }
}
