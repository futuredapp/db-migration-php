<?php

namespace TheFuntasty\Migration;

use Nette\Database\Context;
use TheFuntasty\Migration\Exception\InvalidSqlDirectoryException;
use TheFuntasty\Migration\Exception\MigrationException;

final class Migration
{
    private $context;
    private $sqlDir;
    private $table;

    public function __construct(Context $context, string $sqlDir, string $table = '_migrations')
    {
        self::checkSqlDir($sqlDir);
        $this->context = $context;
        $this->sqlDir = $sqlDir;
        $this->table = $table;
    }

    /**
     * @throws MigrationException
     */
    public function run()
    {
        try {
            $this->context->beginTransaction();

            $this->createMigrationTable();

            $latestMigration = $this->context->table($this->table)
                ->select('number')
                ->order('number DESC')
                ->limit(1)
                ->fetchField('number');

            if ($latestMigration == null) {
                $result = $this->migrate();
            } else {
                $result = $this->migrate($latestMigration);
            }

            $this->context->commit();
        } catch (\Exception $e) {
            $this->context->rollBack();
            throw new MigrationException($e->getMessage(), $e->getCode(), $e);
        }

        return $result;
    }

    /**
     * @throws MigrationException
     */
    private function migrate(int $from = 0): array
    {
        $result = [];

        foreach (scandir($this->sqlDir) as $file) {
            if ($file !== '..' && $file !== '.') {
                preg_match('/([0-9]{4})_.*\.sql/', $file, $matches);

                if (!is_numeric($matches[1])) {
                    throw new MigrationException('File must be in format `0000_Name.sql`');
                }

                $number = (int)$matches[1];

                if ($from >= $number) {
                    // Already migrated
                } else {
                    if ($from + 1 != $number) {
                        throw new MigrationException(sprintf('Next migration should start with `%d`', $from + 1));
                    }

                    $this->migrateFile($number, $file);
                    $result[] = $file;
                    $from++;
                }
            }
        }

        return $result;
    }

    private function migrateFile(int $number, string $fileName): void
    {
        $this->context->query(file_get_contents($this->sqlDir . '/' . $fileName));
        $this->context->table($this->table)
            ->insert([
                'number' => $number,
                'file' => $fileName,
                'created' => new \DateTime,
            ]);
    }

    private function createMigrationTable()
    {
        $this->context->query('
            CREATE TABLE IF NOT EXISTS ?name (
            `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
            `number` int(10) unsigned NOT NULL,
            `file` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
            `created` datetime NOT NULL,
            PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;',
            $this->table
        );
    }

    private static function checkSqlDir($sqlDir)
    {
        if (!is_dir($sqlDir)) {
            throw new InvalidSqlDirectoryException();
        }
    }
}
