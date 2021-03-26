package csxdatastore

import (
	"encoding/json"
	"reflect"
	"sync"

	deepcopier "github.com/mohae/deepcopy"
	"github.com/sirupsen/logrus"
)

// DataStore store local data
type DataStore struct {
	sync.RWMutex
	name        string
	propID      string
	propIndexes []string
	load        DataStoreLoadProc
	items       sync.Map
	indexes     sync.Map
}

// DataStoreLoadProc load store items function
type DataStoreLoadProc func(id *string) (interface{}, error)

// NewDataStore create DataStore
func NewDataStore(storeName, propID string, indexes []string, load DataStoreLoadProc) *DataStore {
	return &DataStore{
		name:        storeName,
		propID:      propID,
		propIndexes: indexes,
		load:        load,
	}
}

// Find data store search element by id
func (store *DataStore) Find(args ...interface{}) (result interface{}, ok bool) {
	var propID string
	var propVal interface{}
	argcnt := len(args)
	if argcnt > 1 {
		propVal = args[0]
		propIDValue := reflect.Indirect(reflect.ValueOf(args[1]))
		if propIDValue.IsValid() {
			propID = propIDValue.String()
		}
		if propID == "" {
			logrus.Error("store '" + store.name + "' invalid index id")
			return nil, false
		}
	} else if argcnt > 0 {
		propVal = args[0]
	}
	if propVal == nil {
		return nil, false
	}

	if propID == "" {
		val := reflect.Indirect(reflect.ValueOf(propVal))
		if val.IsValid() {
			id := val.String()
			if id == "" {
				ok = false
			} else {
				result, ok = store.items.Load(id)
				if !ok {
					logrus.Warn("store '"+store.name+"' not found: ", id)
				}
			}
		}
	} else {
		indexInt, isIndex := store.indexes.Load(propID)
		if isIndex {
			index, isValid := indexInt.(*sync.Map)
			if isValid {
				result, ok = index.Load(propVal)
			}
		} else {
			logrus.Error("store '"+store.name+"' not found index: ", propID)
		}
	}
	return result, ok
}

// Load data store from source
func (store *DataStore) Load() {
	items, err := store.load(nil)
	if err != nil {
		panic("store '" + store.name + "' load " + err.Error())
	}
	itemsVal := reflect.ValueOf(items)
	cnt := itemsVal.Len()
	cntIndexes := len(store.propIndexes)
	for i := 0; i < cntIndexes; i++ {
		indexesMap := sync.Map{}
		store.RLock()
		store.indexes.Store(store.propIndexes[i], &indexesMap)
		store.RUnlock()
	}
	for i := 0; i < cnt; i++ {
		itemVal := itemsVal.Index(i)
		itemPtr := itemVal.Addr().Interface()
		itemID := itemVal.FieldByName(store.propID).String()
		store.items.Store(itemID, itemPtr)
		if cntIndexes > 0 {
			store.updateIndexies(itemVal, itemPtr, nil)
		}
	}
}

func (store *DataStore) Items() *sync.Map {
	return &store.items
}

// Range iterate datastore map and run callback
// func (store *DataStore) Range(cb func(key, val interface{}) bool) {
// 	for key, val := range store.items {
// 		res := cb(key, val)
// 		if !res {
// 			break
// 		}
// 	}
// }

// Update data store by data
func (store *DataStore) Update(cmd, id, data string) {
	var itemPtr interface{}
	var itemVal reflect.Value
	var oldValues []interface{}
	cntIndexes := len(store.propIndexes)
	isUpdate := cmd == "update"
	if isUpdate {
		var ok bool
		itemPtr, ok = store.items.Load(id)
		if !ok || itemPtr == nil {
			logrus.Error("store '"+store.name+"' not found: ", id)
			return
		}
	}
	if itemPtr == nil {
		items, err := store.load(&id)
		if err != nil {
			logrus.Error("store '"+store.name+"' load: ", id, " ", err)
		} else {
			itemsVal := reflect.ValueOf(items)
			if itemsVal.Len() == 0 {
				logrus.Error("store '"+store.name+"' not found: ", id)
			} else {
				itemVal = itemsVal.Index(0)
				itemPtr = itemVal.Addr().Interface()
			}
		}
	} else {
		var err error
		if cntIndexes > 0 {
			itemVal = reflect.ValueOf(itemPtr)
			if itemVal.Kind() == reflect.Ptr {
				itemVal = reflect.Indirect(itemVal)
			}
			oldValues = store.readIndexies(itemVal, itemPtr)
		}
		writeItem(itemPtr, func(locked bool) {
			if !locked {
				// Делаем полную копию, если у элемента нет методов блокировки (можно использовать только для чтения)
				itemPtr = deepcopier.Copy(itemPtr)
				itemVal = reflect.Indirect(reflect.ValueOf(itemPtr))
			}
			err = json.Unmarshal([]byte(data), itemPtr)
		})
		if err != nil {
			logrus.Error("store '"+store.name+"' unmarshal: ", id, " ", err)
			itemPtr = nil
		}
	}
	if itemPtr != nil {
		store.items.Store(id, itemPtr)
		if cntIndexes > 0 {
			store.updateIndexies(itemVal, itemPtr, oldValues)
		}
		logrus.Warn("store '"+store.name+"' update: ", id, " ", data)
	}
}

func (store *DataStore) readIndexies(itemVal reflect.Value, itemPtr interface{}) []interface{} {
	cnt := len(store.propIndexes)
	result := make([]interface{}, cnt)
	readItem(itemPtr, func() {
		for i := 0; i < cnt; i++ {
			store.RLock()
			propID := store.propIndexes[i]
			store.RUnlock()
			prop := reflect.Indirect(itemVal.FieldByName(propID))
			if !prop.IsValid() {
				continue
			}
			result[i] = prop.Interface()
		}
	})
	return result
}

func (store *DataStore) updateIndexies(itemVal reflect.Value, itemPtr interface{}, oldValues []interface{}) {
	cnt := len(store.propIndexes)
	for i := 0; i < cnt; i++ {
		store.RLock()
		propID := store.propIndexes[i]
		store.RUnlock()
		prop := reflect.Indirect(itemVal.FieldByName(propID))
		indexInt, isIndex := store.indexes.Load(propID)
		if !isIndex || !prop.IsValid() {
			continue
		}
		index, ok := indexInt.(*sync.Map)
		if !ok {
			continue
		}
		propVal := prop.Interface()
		if oldValues != nil {
			oldVal := oldValues[i]
			if oldVal != propVal && oldVal != nil {
				index.Delete(oldVal)
			}
		}
		if propVal != nil {
			index.Store(propVal, itemPtr)
		}
	}
}

func writeItem(itemPtr interface{}, cb func(locked bool)) {
	itemVal := reflect.ValueOf(itemPtr)
	lock := itemVal.MethodByName("Lock")
	locked := lock.IsValid()
	if locked {
		lock.Call([]reflect.Value{})
	}
	cb(locked)
	if locked {
		unlock := itemVal.MethodByName("Unlock")
		unlock.Call([]reflect.Value{})
	}
}

func readItem(itemPtr interface{}, cb func()) {
	itemVal := reflect.ValueOf(itemPtr)
	lock := itemVal.MethodByName("RLock")
	if lock.IsValid() {
		lock.Call([]reflect.Value{})
	}
	cb()
	if lock.IsValid() {
		unlock := itemVal.MethodByName("RUnlock")
		unlock.Call([]reflect.Value{})
	}
}
