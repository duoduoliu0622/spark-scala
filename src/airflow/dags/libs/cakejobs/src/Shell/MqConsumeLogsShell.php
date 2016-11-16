<?php
/**
 * CakePHP(tm) : Rapid Development Framework (http://cakephp.org)
 * Copyright (c) Cake Software Foundation, Inc. (http://cakefoundation.org)
 *
 * Licensed under The MIT License
 * For full copyright and license information, please see the LICENSE.txt
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright Copyright (c) Cake Software Foundation, Inc. (http://cakefoundation.org)
 * @link      http://cakephp.org CakePHP(tm) Project
 * @since     3.0.0
 * @license   http://www.opensource.org/licenses/mit-license.php MIT License
 */
namespace App\Shell;

use Api;
use App\Model\Behavior\Credentials;
use App\Model\Behavior\Queries\QueryEngine;
use App\Model\Behavior\Queries\RowQuoting;
use Cake\Console\Shell;
use Curl\Curl;

/**
 * Simple console wrapper around Psy\Shell.
 */
class MqConsumeLogsShell extends Shell
{
    const SECTION_MQ = "rabbitmq";

    public function main()
    {
        /** @var Credentials $credentials */
        $credentials = Api::get('credentials');

        $rows = $this->consumeQueue($credentials);
        foreach ($rows as $row) {
            $this->save($row);
        }
    }

    private function consumeQueue(Credentials $credentials)
    {
        /** @var Curl $curl */
        $curl = Api::get('curl');
        $curl->setBasicAuthentication(
            $credentials->get(self::SECTION_MQ, 'username'),
            $credentials->get(self::SECTION_MQ, 'password')
        );

        $body = json_encode([
            'count' => 10,
            'requeue' => false,
            'encoding' => 'auto',
        ]);
        $messages = $curl->post($credentials->get(self::SECTION_MQ, 'logs_consume_url'), $body);
        if ($curl->error) {
            throw new \Exception("error code: " . $curl->errorCode . ",  " . $curl->errorMessage);
        }

        $rows = [];
        foreach ($messages as $msg) {
            preg_match('/\[(.*)\](.*)/', $msg->payload, $matches);
            $rows[] = ['the_date' => $matches[1], 'time_elapsed' => $matches[2]];
        }

        return $rows;
    }

    private function save($row)
    {
        /** @var QueryEngine $engine */
        $engine = Api::get('query.engine');

        /** @var RowQuoting $rowQuoting */
        $rowQuoting = Api::get('query.row.quoting');
        $row = $rowQuoting->doubleQuote($row);
        $partSql = implode(",", $row);
        $sql = "insert into mq_netsuite_response_time values($partSql)";
        var_dump($sql);

        $engine->insertOrUpdate('default', $sql);
    }
}
