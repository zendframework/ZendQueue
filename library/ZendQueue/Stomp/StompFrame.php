<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Queue
 */

namespace ZendQueue\Stomp;

/**
 * This interface represents a Stomp Frame
 *
 * @category   Zend
 * @package    Zend_Queue
 * @subpackage Stomp
 */
interface StompFrame
{
    /**
     * Get the status of the auto content length
     *
     * If AutoContentLength is true this code will automatically put the
     * content-length header in, even if it is already set by the user.
     *
     * This is done to make the message sending more reliable.
     *
     * @return boolean
     */
    public function getAutoContentLength();

    /**
     * setAutoContentLength()
     *
     * Set the value on or off.
     *
     * @param boolean $auto
     * @return StompFrame
     * @throws \ZendQueue\Exception\ExceptionInterface
     */
    public function setAutoContentLength($auto);

    /**
     * Get the headers
     *
     * @return array
     */
    public function getHeaders();

    /**
     * Set the headers
     *
     * Throws an exception if the array values are not strings.
     *
     * @param array $headers
     * @return StompFrame
     * @throws \ZendQueue\Exception\ExceptionInterface
     */
    public function setHeaders(array $headers);

    /**
     * Returns a value for a header
     * returns false if the header does not exist
     *
     * @param string $header
     * @return $string
     * @throws \ZendQueue\Exception\ExceptionInterface
     */
    public function getHeader($header);

    /**
     * Returns a value for a header
     * returns false if the header does not exist
     *
     * @param string $header
     * @param string $value
     * @return StompFrame
     * @throws \ZendQueue\Exception\ExceptionInterface
     */
    public function setHeader($header, $value);

    /**
     * Return the body for this frame
     * returns false if the body does not exist
     *
     * @return StompFrame
     */
    public function getBody();

    /**
     * Set the body for this frame
     * returns false if the body does not exist
     *
     * Set to null for no body.
     *
     * @param string|null $body
     * @return StompFrame
     * @throws \ZendQueue\Exception\ExceptionInterface
     */
    public function setBody($body);

    /**
     * Return the command for this frame
     * return false if the command does not exist
     *
     * @return StompFrame
     */
    public function getCommand();

    /**
     * Set the body for this frame
     * returns false if the body does not exist
     *
     * @return StompFrame
     * @throws \ZendQueue\Exception\ExceptionInterface
     */
    public function setCommand($command);


    /**
     * Takes the current parameters and returns a Stomp Frame
     *
     * @throws \ZendQueue\Exception\ExceptionInterface
     * @return string
     */
    public function toFrame();

    /**
     * @see toFrame()
     */
    public function __toString();

    /**
     * Accepts a frame and deconstructs the frame into its' component parts
     *
     * @param string $frame - a stomp frame
     * @return StompFrame
     */
    public function fromFrame($frame);
}
