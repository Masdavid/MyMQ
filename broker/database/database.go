package database

import (
	"github.com/dgraph-io/badger"
	"time"
	// "fmt"
	"broker/someTypes"
)

type BasicDb struct {
	db *badger.DB
}

//badger是一种非关系型数据库key-value型的，Open函数返回一个数据库接口的引用
func NewBasicDb(stPath string) *BasicDb {
	st := &BasicDb{}
	opts := badger.DefaultOptions(stPath)
	opts.SyncWrites = true
	opts.Dir = stPath
	opts.ValueDir = stPath
	var err error
	st.db, err = badger.Open(opts)
	if err != nil {
		panic(err)
	}

	go st.runGC()
	return st
}

func (st *BasicDb) Close() error {
	return st.db.Close()
}

//垃圾回收
func (st *BasicDb) runGC(){
	tick := time.NewTicker(10*time.Minute)
	defer tick.Stop()
	for range tick.C {
	again:
 	err := st.db.RunValueLogGC(0.7)
		if err == nil {
			goto again
		}
	}
}

// set函数的实现
func (st *BasicDb) Set(key string, value []byte) (err error){
	return st.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), value)
		return err
	})
}

//删除函数的实现
func (st *BasicDb) Delete(key string) (err error) {
	return st.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		return err
	})
}

//查询函数的实现
func (st *BasicDb) Get(key string) (val []byte, err error) {
	err = st.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get([]byte(key)) 
		if err != nil {
			return err
		}
		val, err = it.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

//遍历所有key-value对
func (st *BasicDb) Iterate(f func(key []byte, val []byte)) {
	st.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			f(k, val)
		}
		return nil
	})
}

//根据前缀遍历key与value
func (st *BasicDb) IterateWithPre(pre []byte, f func(key []byte, value []byte)) uint64 {
	var num uint64
	st.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(pre); it.ValidForPrefix(pre); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			f(k, v)
			num++
		}
		return nil
	})
	return num
}

//带有限制地遍历key-value
func (st *BasicDb) IterateWithPreLimit(pre []byte, up uint64, f func(key []byte, value []byte)) uint64 {
	var num uint64
	st.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(pre); it.ValidForPrefix(pre) && ((up > 0 && num < up) || up <= 0); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			f(k, v)
			num++
		}
		return nil
	})
	return num
}

//根据前缀遍历key与value
func (st *BasicDb) IterateWithPreFrom(pre []byte, from []byte, up uint64, f func(key []byte, value []byte)) uint64 {
	var num uint64
	st.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(from); it.ValidForPrefix(pre) && ((up > 0 && num < up) || up <= 0); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			f(k, v)
			num++
		}
		return nil
	})
	return num
}

//根据前缀计算key的个数
func (st *BasicDb) keyCountWithPre(pre []byte) uint64 {
	var num uint64
	st.db.View(func (txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		it := txn.NewIterator(opts)
		defer it.Close()
		
		for it.Seek(pre); it.ValidForPrefix(pre); it.Next() {
			num++
		}
		return nil
	})
	return num
}

//根据前缀删除key， 首先得到以pre为前缀的key，然后再删除
func (st *BasicDb) DeleteKeyWithPre(pre []byte) {
	deleteKeys := func(keys [][]byte) error {
		if err := st.db.Update(func (txn *badger.Txn) error {
			for _, key := range keys {
				if err := txn.Delete(key); err != nil {
					return err
				}
			} 
			return nil
		}); err != nil{
			return err
		}
		return nil
	}


	//每个事务删除十万条key
	eachLen := 100000
	eachKeys := make([][]byte, 0, eachLen)
	allKeys := make([][][]byte, 0)
	curNum := 0

	st.db.View(func(txn *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()


		for it.Seek(pre); it.ValidForPrefix(pre); it.Next() {

			item := it.Item()
			k := item.KeyCopy(nil)
			eachKeys = append(eachKeys, k)
			curNum++

			if(curNum == eachLen) {
				allKeys = append(allKeys, eachKeys)
				eachKeys = make([][]byte, 0, eachLen)
				curNum = 0
			}
		}
		if curNum > 0 {
			allKeys = append(allKeys, eachKeys)
		}
		return nil
	})


	for _, keys := range allKeys {
		deleteKeys(keys)
	} 
}

//批处理
func (st *BasicDb) BatchHandle(ops []*someTypes.Operation) (err error) {
	err = st.db.Update(func(txn *badger.Txn) error{
		for _, cur := range ops {
			if cur.Op == someTypes.OpSet {
				// fmt.Println("key of message is ", cur.Key)
				if err := txn.Set([]byte(cur.Key), cur.Value); err != nil {
					return err
				}
			}else if cur.Op == someTypes.OpDel {
				if err := txn.Delete([]byte(cur.Key)); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return
}

