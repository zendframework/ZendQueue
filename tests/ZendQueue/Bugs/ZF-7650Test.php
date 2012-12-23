<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Queue
 */

namespace ZendQueueTest\Bugs;

use ZendQueue\Queue;

/*
 * This code specifically tests for ZF-7650
 */

/**
 * @category   Zend
 * @package    Zend_Queue
 * @subpackage UnitTests
 * @group      Zend_Queue
 */
class Zf7650Test extends \PHPUnit_Framework_TestCase
{
    public function testArrayAdapterShouldReturnNoMessagesWhenZeroCountRequested()
    {
        // \ZendQueue\Adapter\ArrayAdapter
        $queue = new Queue('ArrayAdapter');
        $queue2 = $queue->createQueue('queue');

        $queue->send('My Test Message 1');
        $queue->send('My Test Message 2');

        $messages = $queue->receive(0);
        $this->assertEquals(0, count($messages));
    }

    public function testMemcacheqAdapterShouldReturnNoMessagesWhenZeroCountRequested()
    {
        if (
            !defined('TESTS_ZEND_QUEUE_MEMCACHEQ_ENABLED') ||
            !constant('TESTS_ZEND_QUEUE_MEMCACHEQ_ENABLED')
        ) {
            $this->markTestSkipped('Zend_Queue Memcacheq adapter tests are not enabled');
        }
        $driverOptions = array();
        if (defined('TESTS_ZEND_QUEUE_MEMCACHEQ_HOST')) {
            $driverOptions['host'] = TESTS_ZEND_QUEUE_MEMCACHEQ_HOST;
        }
        if (defined('TESTS_ZEND_QUEUE_MEMCACHEQ_PORT')) {
            $driverOptions['port'] = TESTS_ZEND_QUEUE_MEMCACHEQ_PORT;
        }
        $options = array('name' => 'ZF7650', 'driverOptions' => $driverOptions);

        $queue = new Queue('Memcacheq', $options);
        $queue2 = $queue->createQueue('queue');

        $queue->send('My Test Message 1');
        $queue->send('My Test Message 2');

        $messages = $queue->receive(0);
        $this->assertEquals(0, count($messages));

    }

    public function testDbAdapterShouldReturnNoMessagesWhenZeroCountRequested()
    {
        if (
            !defined('TESTS_ZEND_QUEUE_DB_ENABLED') ||
            !constant('TESTS_ZEND_QUEUE_DB_ENABLED')
        ) {
            $this->markTestSkipped('Zend_Queue DB adapter tests are not enabled');
        }
        $driverOptions = array();
        if (defined('TESTS_ZEND_QUEUE_DB')) {
            $driverOptions = \Zend\Json\Json::decode(TESTS_ZEND_QUEUE_DB);
        }

        $options = array(
            'name'          => '/temp-queue/ZF7650',
            'options'       => array(\Zend\DB\Select::FOR_UPDATE => true),
            'driverOptions' => $driverOptions,
        );

        $queue = new Queue('Db', $options);
        $queue2 = $queue->createQueue('queue');

        $queue->send('My Test Message 1');
        $queue->send('My Test Message 2');

        $messages = $queue->receive(0);
        $this->assertEquals(0, count($messages));
    }

    public function testActivemqAdapterShouldReturnNoMessagesWhenZeroCountRequested()
    {
        if (
            !defined('TESTS_ZEND_QUEUE_ACTIVEMQ_ENABLED') ||
            !constant('TESTS_ZEND_QUEUE_ACTIVEMQ_ENABLED')
        ) {
            $this->markTestSkipped('Zend_Queue ActiveMQ adapter tests are not enabled');
        }
        $driverOptions = array();
        if (defined('TESTS_ZEND_QUEUE_ACTIVEMQ_HOST')) {
            $driverOptions['host'] = TESTS_ZEND_QUEUE_ACTIVEMQ_HOST;
        }
        if (defined('TESTS_ZEND_QUEUE_ACTIVEMQ_PORT')) {
            $driverOptions['port'] = TESTS_ZEND_QUEUE_ACTIVEMQ_PORT;
        }
        if (defined('TESTS_ZEND_QUEUE_ACTIVEMQ_SCHEME')) {
            $driverOptions['scheme'] = TESTS_ZEND_QUEUE_ACTIVEMQ_SCHEME;
        }
        $options = array('driverOptions' => $driverOptions);

        $queue = new Queue('Activemq', $options);
        $queue2 = $queue->createQueue('queue');

        $queue->send('My Test Message 1');
        $queue->send('My Test Message 2');

        $messages = $queue->receive(0);
        $this->assertEquals(0, count($messages));
    }
}
