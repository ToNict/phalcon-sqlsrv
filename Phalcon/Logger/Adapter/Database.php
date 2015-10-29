<?php

namespace Phalcon\Logger\Adapter;

use Phalcon\Logger\Exception;
use Phalcon\Logger\Formatter\Line as LineFormatter;
use Phalcon\Logger\Adapter as LoggerAdapter;
use Phalcon\Logger\AdapterInterface;

/**
 * Phalcon\Logger\Adapter\Database
 * Adapter to store logs in a database table.
 */
class Database extends LoggerAdapter implements AdapterInterface
{
    /**
     * Username.
     *
     * @var string
     */
    protected $username = 'guest';

    /**
     * Adapter options.
     *
     * @var array
     */
    protected $options = [];

    /**
     * Class constructor.
     *
     * @param string $name
     * @param array  $options
     *
     * @throws \Phalcon\Logger\Exception
     */
    public function __construct(array $options = [])
    {
        if (!isset($options['db'])) {
            throw new Exception("Parameter 'db' is required");
        }

        if (!isset($options['table'])) {
            throw new Exception("Parameter 'table' is required");
        }

        if (!empty($options['username'])) {
            $this->username = $options['username'];
        }

        $this->options = $options;
    }

    /**
     * {@inheritdoc}
     *
     * @return \Phalcon\Logger\FormatterInterface
     */
    public function getFormatter()
    {
        if (!is_object($this->_formatter)) {
            $this->_formatter = new LineFormatter();
        }

        return $this->_formatter;
    }

    /**
     * Writes the log to the file itself.
     *
     * @param string $message
     * @param int    $type
     * @param int    $time
     * @param array  $context
     */
    public function logInternal($message, $type, $time, $context)
    {
        //        return $this->options['db']->execute(
//                'INSERT INTO ' . $this->options['table'] . ' (LogType, LogProcess, LogContent, LogUser, LogDate, LogIP, LogBrowser) VALUES (?, ?, ?, ?, ?, ?, ?)', [$type, $context['process'], $message, $this->username, date('Y-m-d H:i:s', $time), $this->getIP(), $this->getBrowser()]
//        );
        return $this->options['db']->insertAsDict(
                $this->options['table'], array(
                'LogType' => $type,
                'LogProcess' => $context['process'],
                'LogContent' => $message,
                'LogUser' => $this->username,
                'LogDate' => date('Y-m-d H:i:s', $time),
                'LogIP' => $this->getIP(),
                'LogBrowser' => $this->getBrowser(),
        ));
    }

    /**
     * Closes the logger.
     *
     * @return bool
     */
    public function close()
    {
        return true;
    }

    public function getIP()
    {
        return (getenv(HTTP_X_FORWARDED_FOR)) ? getenv(HTTP_X_FORWARDED_FOR) : getenv(REMOTE_ADDR);
    }

    public function getBrowser()
    {
        // Declare known browsers to look for
        $browsers = array('chrome', 'firefox', 'safari', 'msie', 'opera',
            'mozilla', 'seamonkey', 'konqueror', 'netscape',
            'gecko', 'navigator', 'mosaic', 'lynx', 'amaya',
            'omniweb', 'avant', 'camino', 'flock', 'aol', );

        // Find all phrases (or return empty array if none found)
        foreach ($browsers as $browser) {
            if (preg_match("#($browser)[/ ]?([0-9.]*)#", strtolower($_SERVER['HTTP_USER_AGENT']), $match)) {
                $info['name'] = $match[1];
                $info['version'] = $match[2];
                break;
            }
        }

        return "{$info['name']} ({$info['version']})";
    }
}
