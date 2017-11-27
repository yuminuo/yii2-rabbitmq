<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace adamyxt\rabbitmq;

use yii\console\Controller;

/**
 * Class Command
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
abstract class CliCommand extends Controller
{
    /**
     * @var Queue
     */
    public $queue;
    /**
     * @var bool verbose mode of a job execute. If enabled, execute result of each job
     * will be printed.
     */
    public $verbose = false;
    /**
     * @var bool isolate mode. It executes a job in a child process.
     */
    public $isolate = true;


    /**N
     * @inheritdoc
     */
    public function options($actionID)
    {
        $options = parent::options($actionID);
        if ($this->useVerboseOption($actionID)) {
            $options[] = 'verbose';
        }
        if ($this->useIsolateOption($actionID)) {
            $options[] = 'isolate';
        }

        return $options;
    }

    /**
     * @inheritdoc
     */
    public function optionAliases()
    {
        return array_merge(parent::optionAliases(), [
            'v' => 'verbose',
        ]);
    }

    /**
     * @param string $actionID
     * @return bool
     */
    protected function useVerboseOption($actionID)
    {
        return in_array($actionID, ['exec', 'run', 'listen']);
    }

    /**
     * @param string $actionID
     * @return bool
     */
    protected function useIsolateOption($actionID)
    {
        return in_array($actionID, ['run', 'listen']);
    }

}