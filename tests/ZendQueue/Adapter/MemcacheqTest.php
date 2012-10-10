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
class MemcacheqTest extends AdapterTest
{
    /**
     * getAdapterName() is an method to help make AdapterTest work with any
     * new adapters
     *
     * You must overload this method
     *
     * @return string
     */
    public function getAdapterName()
    {
        return 'Memcacheq';
    }

    /**
     * getAdapterName() is an method to help make AdapterTest work with any
     * new adapters
     *
     * You may overload this method.  The default return is
     * 'Zend_Queue_Adapter_' . $this->getAdapterName()
     *
     * @return string
     */
    public function getAdapterFullName()
    {
        return '\ZendQueue\Adapter\\' . $this->getAdapterName();
    }

    public function getTestConfig()
    {
        $driverOptions = array();
        if (defined('TESTS_ZEND_QUEUE_MEMCACHEQ_HOST')) {
            $driverOptions['host'] = TESTS_ZEND_QUEUE_MEMCACHEQ_HOST;
        }
        if (defined('TESTS_ZEND_QUEUE_MEMCACHEQ_PORT')) {
            $driverOptions['port'] = TESTS_ZEND_QUEUE_MEMCACHEQ_PORT;
        }
        return array('driverOptions' => $driverOptions);
    }

    // test the constants
    public function testConst()
    {
        $this->assertTrue(is_string(Adapter\Memcacheq::DEFAULT_HOST));
        $this->assertTrue(is_integer(Adapter\Memcacheq::DEFAULT_PORT));
        $this->assertTrue(is_string(Adapter\Memcacheq::EOL));
    }
    
    public function testIsExistsCompatibility()
    {
        $previousArrayContent = array("testCount", "testDeleteMessage", "testZendQueueAdapterConstructor", "ZF7650", "queue1", "queue", "testFactory", "foo");
        $currentArrayContent = array("testCount 12/12", "testDeleteMessage 12/12", "testZendQueueAdapterConstructor 12/12", "ZF7650 50/2", "queue1 2/2", "queue 6/2", "testFactory 12/12", "foobar 2/2");
    
        $adapter = $this->getMock('ZendQueue\Adapter\Memcacheq', array('getQueues'), array(), "", false);
        $adapter->expects($this->any())->method('getQueues')->will($this->returnValue($previousArrayContent));
        $reflection = new \ReflectionObject($adapter);
        $property = $reflection->getProperty("_queues");
        $property->setAccessible(true);
        $property->setValue($adapter, $previousArrayContent);
    
        $this->assertTrue($adapter->isExists("testCount"));
        $this->assertFalse($adapter->isExists("testCountFoo"));
    
        $property->setValue($adapter, $currentArrayContent);
        $this->assertTrue($adapter->isExists("testCount"));
        $this->assertFalse($adapter->isExists("testCountFoo"));
    
        $this->assertFalse($adapter->isExists("foo"));
    }
}
