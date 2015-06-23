<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Queue
 */

namespace ZendQueueTest\Adapter;

use ZendQueue\Queue;
use ZendQueue\Adapter;

/**
 * @category   Zend
 * @package    ZendQueue\Queue
 * @subpackage UnitTests
 */
class AmqpTest extends AdapterTest
{
    /**
     * (non-PHPdoc)
     * @see PHPUnit_Framework_TestCase::tearDown()
     */
    public function tearDown()
    {
        if (!$queue = $this->createQueue($this->getName())) {
            return;
        }
        $adapter = $queue->getAdapter();
        $adapter->getQueue()->deleteQueue();
        parent::tearDown();
    }

    /**
     * (non-PHPdoc)
     * @see \ZendQueueTest\Adapter\AdapterTest::getAdapterName()
     */
    public function getAdapterName()
    {
        return 'Amqp';
    }

    /**
     * (non-PHPdoc)
     * @see \ZendQueueTest\Adapter\AdapterTest::getAdapterFullName()
     */
    public function getAdapterFullName()
    {
        return '\ZendQueue\Adapter\\' . $this->getAdapterName();
    }

    /**
     * (non-PHPdoc)
     * @see \ZendQueueTest\Adapter\AdapterTest::getTestConfig()
     */
    public function getTestConfig()
    {
        $driverOptions = array();
        if (defined('TESTS_ZEND_QUEUE_AMQP_HOST')) {
            $driverOptions['host'] = TESTS_ZEND_QUEUE_AMQP_HOST;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_PORT')) {
            $driverOptions['port'] = TESTS_ZEND_QUEUE_AMQP_PORT;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_USER')) {
            $driverOptions['user'] = TESTS_ZEND_QUEUE_AMQP_USER;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_PASS')) {
            $driverOptions['pass'] = TESTS_ZEND_QUEUE_AMQP_PASS;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_VHOST')) {
            $driverOptions['vhost'] = TESTS_ZEND_QUEUE_AMQP_VHOST;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_DEBUG')) {
            $driverOptions['debug'] = TESTS_ZEND_QUEUE_AMQP_DEBUG;
        }
        return array('driverOptions' => $driverOptions);
    }

    /**
     *
     */
    public function testFailedConstructor()
    {
        try {
            $queue = $this->createQueue(__FUNCTION__, array());
            /*
             * delete the queue we created
             */
            $queue->deleteQueue();
            $this->fail('The test should fail if no host and password are passed');
        } catch (\Exception $e) {
            $this->assertTrue( true, 'Job Queue host and password should be provided');
        }
    }

    /**
     * (non-PHPdoc)
     * @see \ZendQueueTest\Adapter\AdapterTest::testConst()
     */
    public function testConst()
    {
        $this->assertTrue(is_string(Adapter\Amqp::DEFAULT_HOST));
        $this->assertTrue(is_integer(Adapter\Amqp::DEFAULT_PORT));
        $this->assertTrue(is_string(Adapter\Amqp::DEFAULT_USER));
        $this->assertTrue(is_string(Adapter\Amqp::DEFAULT_PASS));
        $this->assertTrue(is_string(Adapter\Amqp::DEFAULT_VHOST));
        $this->assertTrue(is_bool(Adapter\Amqp::DEFAULT_DEBUG));
    }

    /**
     * (non-PHPdoc)
     * @see \ZendQueueTest\Adapter\AdapterTest::testSampleBehavior()
     */
    public function testSampleBehavior()
    {
        if (!$queue = $this->createQueue(__FUNCTION__)) {
            return;
        }
        $this->assertTrue($queue instanceof Queue);

        if ($queue->isSupported('send')) {
            $msg = 1;

            for($i = 0; $i < 10; $i++) {
                $queue->send("$msg");
                $msg ++;
            }
        }

        if ($queue->isSupported('receive')) {
            $msg = 1;
            $messages = $queue->receive(5);
            foreach($messages as $i => $message) {
                $this->assertEquals($msg, $message->body);
                $queue->deleteMessage($message);
                $msg++;
            }
        }

        $this->assertEquals(5, count($queue));
        $this->assertTrue($queue->deleteQueue());
        /*
         * delete the queue we created
         */
        $queue->deleteQueue();
    }
}