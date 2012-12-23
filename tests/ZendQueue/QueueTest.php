<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Queue
 */

namespace ZendQueueTest;

use ZendQueue\Queue;
use Zend\Log;
use Zend\Log\Writer;
use ZendQueue\Adapter;

/*
 * The adapter test class provides a universal test class for all of the
 * abstract methods.
 *
 * All methods marked not supported are explictly checked for for throwing
 * an exception.
 */

/**
 * @category   Zend
 * @package    Zend_Queue
 * @subpackage UnitTests
 * @group      Zend_Queue
 */
class QueueTest extends \PHPUnit_Framework_TestCase
{
    protected function setUp()
    {
        // Test Zend_Config
        $this->config = array(
            'name'      => 'queue1',
            'params'    => array(),
        );

        $this->queue = new Queue('ArrayAdapter', $this->config);
    }

    protected function tearDown()
    {
    }

    public function testConst()
    {
        $this->assertTrue(is_string(Queue::TIMEOUT));
        $this->assertTrue(is_integer(Queue::VISIBILITY_TIMEOUT));
        $this->assertTrue(is_string(Queue::NAME));
    }

    /**
     * Constructor
     *
     * @param string|Zend_Queue_Adapter_Abstract $adapter
     * @param array  $config
     */
    public function testConstruct()
    {
        // Test Zend_Config
        $config = array(
            'name'      => 'queue1',
            'params'    => array(),
            'adapter'   => 'ArrayAdapter'
        );

        $zend_config = new \Zend\Config\Config($config);

        $obj = new Queue($config);
        $this->assertTrue($obj instanceof Queue);

        $obj = new Queue($zend_config);
        $this->assertTrue($obj instanceof Queue);
    }

    public function test_getConfig()
    {
        $options = $this->queue->getOptions();
        $this->assertTrue(is_array($options));
        $this->assertEquals($this->config['name'], $options['name']);
    }

    public function test_set_getAdapter()
    {
        $adapter = new Adapter\ArrayAdapter($this->config);
        $this->assertTrue($this->queue->setAdapter($adapter) instanceof Queue);
        $this->assertTrue($this->queue->getAdapter($adapter) instanceof Adapter\ArrayAdapter);
    }

    public function test_set_getMessageClass()
    {
        $class = 'test';
        $this->assertTrue($this->queue->setMessageClass($class) instanceof Queue);
        $this->assertEquals($class, $this->queue->getMessageClass());
    }

    public function test_set_getMessageSetClass()
    {
        $class = 'test';
        $this->assertTrue($this->queue->setMessageSetClass($class) instanceof Queue);
        $this->assertEquals($class, $this->queue->getMessageSetClass());
    }

    public function test_set_getName()
    {
        // $this->assertTrue($this->queue->setName($new) instanceof ZendQueue_Queue);
        $this->assertEquals($this->config['name'], $this->queue->getName());
    }

    public function test_create_deleteQueue()
    {
        // parameter testing
        try {
            $this->queue->createQueue(array());
            $this->fail('createQueue() $name must be a string');
        } catch (\Exception $e) {
            $this->assertTrue(true);
        }

        try {
            $this->queue->createQueue('test', 'test');
            $this->fail('createQueue() $timeout must be an integer');
        } catch (\Exception $e) {
            $this->assertTrue(true);
        }

        // isExists
        $queue = 'test';
        $new = $this->queue->createQueue($queue);
        $this->assertTrue($new instanceof Queue);
        $this->assertFalse($this->queue->createQueue($queue));

        $this->assertTrue($new->deleteQueue());
    }

    public function test_send_count_receive_deleteMessage()
    {
        // ------------------------------------ send()
        // parameter verification
        try {
            $this->queue->send(array());
            $this->fail('send() $mesage must be a string');
        } catch (\Exception $e) {
            $this->assertTrue(true);
        }

        $message = 'Hello world'; // never gets boring!
        $this->assertTrue($this->queue->send($message) instanceof \ZendQueue\Message);

        // ------------------------------------ count()
        $this->assertEquals($this->queue->count(), 1);

        // ------------------------------------ receive()
        // parameter verification
        try {
            $this->queue->receive(array());
            $this->fail('receive() $maxMessages must be a integer or null');
        } catch (\Exception $e) {
            $this->assertTrue(true);
        }

        try {
            $this->queue->receive(1, array());
            $this->fail('receive() $timeout must be a integer or null');
        } catch (\Exception $e) {
            $this->assertTrue(true);
        }

        $messages = $this->queue->receive();
        $this->assertTrue($messages instanceof \ZendQueue\Message\MessageIterator);

        // ------------------------------------ deleteMessage()
        foreach ($messages as $i => $message) {
            $this->assertTrue($this->queue->deleteMessage($message));
        }
    }

/*
    public function test_set_getLogger()
    {
        $logger = new Log\Logger(new Writer\Null);

        $this->assertTrue($this->queue->setLogger($logger) instanceof Queue);
        $this->assertTrue($this->queue->getLogger() instanceof Log\Logger);

        // parameter verification
        try {
            $this->queue->setLogger(array());
            $this->fail('setlogger() passed an array and succeeded (bad)');
        } catch (\Exception $e) {
            $this->assertTrue(true);
        }
    }
*/

    public function test_capabilities()
    {
        $list = $this->queue->getCapabilities();
        $this->assertTrue(is_array($list));

        // these functions must have an boolean answer
        $func = array(
            'create', 'delete', 'send', 'receive',
            'deleteMessage', 'getQueues', 'count',
            'isExists'
        );

        foreach ( array_values($func) as $f ) {
            $this->assertTrue(isset($list[$f]));
            $this->assertTrue(is_bool($list[$f]));
        }
    }

    public function test_isSupported()
    {
        $list = $this->queue->getCapabilities();
        foreach ( $list as $function => $result ) {
            $this->assertTrue(is_bool($result));
            if ( $result ) {
                $this->assertTrue($this->queue->isSupported($function));
            } else {
                $this->assertFalse($this->queue->isSupported($function));
            }
        }
    }

    public function test_getQueues()
    {
        $queues = $this->queue->getQueues();
        $this->assertTrue(is_array($queues));
        $this->assertTrue(in_array($this->config['name'], $queues));
    }
}
