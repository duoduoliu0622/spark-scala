<?php

namespace App\Model\Behavior\Queries;

use Cake\Datasource\ConnectionManager;

class QueryEngine
{
    public function select($dbAlias, $sql)
    {
        $conn = $this->getConnection($dbAlias);

        return $conn->execute($sql)->fetchAll("assoc");
    }

    public function insertOrUpdate($dbAlias, $sql)
    {
        $conn = $this->getConnection($dbAlias);
        $conn->execute($sql);

        return true;
    }

    /**
     * @param $dbAlias
     * @return \Cake\Datasource\ConnectionInterface
     */
    private function getConnection($dbAlias)
    {
        return ConnectionManager::get($dbAlias);
    }
}