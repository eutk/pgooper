<?php
/**
 * Created by PhpStorm.
 * User: lilei
 * Date: 2019/8/14
 * Time: 10:02 AM
 */

/**
 * @param mixed ...$args
 */

namespace Pgooper;

class DB
{
    const DB_INSET = 1;
    const DB_REPLACE = 2;
    const DB_STORE = 3;

    const DB_BUCKET_SIZE = 262144;
    const DB_KEY_SIZE = 128;
    const DB_INDEX_SIZE = 141; // 末尾加一位状态位

    const DB_KEY_EXISTS = 1;
    const DB_FAILURE = -1;
    const DB_SUCCESS = 0;

    private $idx_fp;
    private $dat_fp;
    private $closed;

    /**
     * @param $string
     * @return int
     */
    private function _hash($string)
    {
        $string = substr(md5($string), 0, 8);
        $hash = 0;
        for ($i = 0; $i < 8; $i++) {
            $hash += 33 * $hash + ord($string{$i});
        }
        return $hash & 0x7FFFFFFF;
    }

    public function open($pathname)
    {
        $idx_path = $pathname . '.idx';
        $dat_path = $pathname . '.dat';

        if (!file_exists($idx_path)) {
            $init = true;
            $mode = 'w+b';
        } else {
            $init = false;
            $mode = 'r+b';
        }

        $this->idx_fp = fopen($idx_path, $mode);
        if (!$this->idx_fp) {
            return self::DB_FAILURE;
        }

        if ($init) {
            $elem = pack('L', 0x00000000);
            for ($i = 0; $i < self::DB_BUCKET_SIZE; $i++) {
                fwrite($this->idx_fp, $elem, 4);
            }
        }

        $this->dat_fp = fopen($dat_path, $mode);
        if (!$this->dat_fp) {
            return self::DB_FAILURE;
        }

        return self::DB_SUCCESS;
    }

    public function fetch($key)
    {
        $offset = (self::_hash($key) % self::DB_BUCKET_SIZE) * 4;

        fseek($this->idx_fp, $offset, SEEK_SET);
        $pos = unpack('L', fread($this->idx_fp, 4));
        $pos = $pos[1];

        $found = false;
        while ($pos) {
            fseek($this->idx_fp, $pos, SEEK_SET);
            $block = fread($this->idx_fp, self::DB_INDEX_SIZE);
            $cpkey = substr($block, 4, self::DB_KEY_SIZE);

            if (!strncmp($key, $cpkey, strlen($key))) {
                $dataOff = unpack('L', substr($block, self::DB_KEY_SIZE + 4, 4));
                $dataOff = $dataOff[1];

                $datalen = unpack('L', substr($block, self::DB_KEY_SIZE + 8, 4));
                $datalen = $datalen[1];

                $found = true;
                break;
            }
            $pos = unpack('L', substr($block, 0, 4));
            $pos = $pos[1];
        }

        if (!$found) {
            return null;
        }

        fseek($this->dat_fp, $dataOff, SEEK_SET);
        $data = fread($this->dat_fp, $datalen);
        return $data;
    }

    public function inster($key, $data)
    {
        $offset = (self::_hash($key) % self::DB_BUCKET_SIZE) * 4;

        $idxoff = fstat($this->idx_fp);
        $idxoff = intval($idxoff['size']);

        $datoff = fstat($this->dat_fp);
        $datoff = intval($datoff['size']);

        $keylen = strlen($key);
        if ($keylen > self::DB_KEY_SIZE) {
            return self::DB_FAILURE;
        }

        $block = pack('L', 0x00000000);
        $block .= $key;
        $space = self::DB_KEY_SIZE - $keylen;
        //实际的键长度小于规定长度，剩余部分用0补齐
        for ($i = 0; $i < $space; $i++) {
            $block .= pack('C', 0x00);
        }
        $block .= pack('L', $datoff);
        $block .= pack('L', strlen($data));
        $block .= 1; // 末尾加状态位

        fseek($this->idx_fp, $offset, SEEK_SET);
        $pos = unpack('L', fread($this->idx_fp, 4));
        $pos = $pos[1];

        if ($pos == 0) {
            fseek($this->idx_fp, $offset, SEEK_SET);
            fwrite($this->idx_fp, pack('L', $idxoff), 4);

            fseek($this->idx_fp, 0, SEEK_END);
            fwrite($this->idx_fp, $block, self::DB_INDEX_SIZE);

            fseek($this->dat_fp, 0, SEEK_END);
            fwrite($this->dat_fp, $data, strlen($data));

            return self::DB_SUCCESS;
        }

        $found = false;
        while ($pos) {
            fseek($this->idx_fp, $pos, SEEK_SET);
            $tmp_block = fread($this->idx_fp, self::DB_INDEX_SIZE);
            $cpkey = substr($tmp_block, 4, self::DB_KEY_SIZE);
            if (!strncmp($key, $cpkey, $keylen)) {
                $found = true;
                break;
            }

            $prev = $pos;
            $pos = unpack('L', substr($tmp_block, 0, 4));
            $pos = $pos[1];
        }

        if ($found) {
            return self::DB_KEY_EXISTS;
        }

        fseek($this->idx_fp, $prev, SEEK_SET);
        fwrite($this->idx_fp, pack('L', $idxoff), 4);
        fseek($this->idx_fp, 0, SEEK_END);
        fwrite($this->idx_fp, $block, self::DB_INDEX_SIZE);
        fseek($this->dat_fp, 0, SEEK_END);
        fwrite($this->dat_fp, $data, strlen($data));
        return self::DB_SUCCESS;
    }

    public function delete($key)
    {
        //计算出键的初始偏移量
        $offset = (self::_hash($key) % self::DB_BUCKET_SIZE) * 4;

        //获取链表指针
        fseek($this->idx_fp, $offset, SEEK_SET);
        $pos = unpack('L', fread($this->idx_fp, 4));
        $pos = $pos[1];

        $prev = 0;
        $next = 0;

        $found = false;
        while ($pos) {
            fseek($this->idx_fp, $pos, SEEK_SET);
            $tmp_block = fread($this->idx_fp, self::DB_INDEX_SIZE);

            $next = unpack('L', substr($tmp_block, 0, 4));
            $next = $next[1];

            $cpkey = substr($tmp_block, 4, self::DB_KEY_SIZE);
            if (!strncmp($key, $cpkey, strlen($key))) {
                $found = true;
                break;
            }

            $prev = $pos;
            $pos = $next;
        }

        if (!$found) {
            return self::DB_FAILURE;
        }

        if ($prev == 0) {
            fseek($this->idx_fp, $offset, SEEK_SET);
            fwrite($this->idx_fp, pack('L', $next), 4);
            fseek($this->idx_fp,$pos + self::DB_INDEX_SIZE - 1,SEEK_SET);
            fwrite($this->idx_fp,0,1);
        } else {
            fseek($this->idx_fp, $prev, SEEK_SET);
            fwrite($this->idx_fp, pack('L', $next), 4);
            fseek($this->idx_fp,$pos + self::DB_INDEX_SIZE - 1,SEEK_SET);
            fwrite($this->idx_fp,0,1);
        }

        return self::DB_SUCCESS;
    }

    private function ergIndex()
    {
        //跳过初始索引块直接拿可用索引
        fseek($this->idx_fp, self::DB_BUCKET_SIZE * 4, SEEK_SET);

        while ($block = fread($this->idx_fp,self::DB_INDEX_SIZE)){
            yield $block;
        }
    }

    public function keys()
    {
        $keys = [];
        $tmp = pack('C', 0x00);

        foreach (self::ergIndex() as $index){
            if (substr($index,-1)) {
                $keys[] = str_replace($tmp,'',substr($index,4,self::DB_KEY_SIZE));
            }
        }

        return $keys;
    }

    public function all()
    {
        $result = [];
        $tmp = pack('C', 0x00);

        foreach (self::ergIndex() as $index) {
            if (substr($index,-1)) {
                $key = str_replace($tmp,'',substr($index,4,self::DB_KEY_SIZE));
                $offset = unpack('L',substr($index,self::DB_KEY_SIZE + 4,4))[1];
                $strlen = unpack('L',substr($index,self::DB_KEY_SIZE + 8,4))[1];
                fseek($this->dat_fp,$offset,SEEK_SET);
                $result[$key] = fread($this->dat_fp,$strlen);
            }
        }

        return $result;
    }

    public function close()
    {
        if (!$this->closed) {
            fclose($this->idx_fp);
            fclose($this->dat_fp);
            $this->closed = true;
        }
    }


}