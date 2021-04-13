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
func (store *DataStore) Find(id interface{}) (result interface{}, ok bool) {
	if id == nil {
		logrus.Warn("store '" + store.name + "', missing id in store.Find(<>) call")
	}
	store.RLock()
	result, ok = store.items.Load(id)
	store.RUnlock()
	return result, ok
}

// Find data store search element by id
func (store *DataStore) FindByIndex(indexID string, value interface{}) (result interface{}, ok bool) {
	if len(indexID) == 0 {
		logrus.Warn("store '" + store.name + "', missing indexID in store.FindByIndex(<>) call")
		return result, ok
	}
	if value == nil {
		logrus.Warn("store '" + store.name + "', missing value in store.FindByIndex(indexID, <>) call")
		return result, ok
	}
	store.RLock()
	// search by index id and value
	indexInt, isIndex := store.indexes.Load(indexID)
	if isIndex {
		index, isValid := indexInt.(*sync.Map)
		if isValid {
			result, ok = index.Load(value)
		}
	} else {
		logrus.Error("store '"+store.name+"' not found index: ", indexID)
	}
	store.RUnlock()
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
		var itemPtr interface{}
		if itemVal.Kind() == reflect.Ptr {
			itemPtr = itemVal.Interface()
			itemVal = reflect.Indirect(itemVal)
		} else {
			itemPtr = itemVal.Addr().Interface()
		}
		itemIDVal := itemVal.FieldByName(store.propID)
		var itemID string
		if itemIDVal.Kind() == reflect.Ptr {
			itemID = reflect.Indirect(itemIDVal).String()
		} else {
			itemID = itemIDVal.String()
		}
		store.items.Store(itemID, itemPtr)
		if cntIndexes > 0 {
			store.updateIndexies(itemPtr, nil)
		}
	}
}

func (store *DataStore) Items() *sync.Map {
	return &store.items
}

// Range iterate datastore map and run callback
func (store *DataStore) Range(cb func(key, val interface{}) bool) {
	store.items.Range(func(key, value interface{}) bool {
		res := cb(key, value)
		return res
	})
}

// Store data store by data
func (store *DataStore) Store(id string, data interface{}) {
	var itemPtr interface{}
	var oldValues []interface{}
	cntIndexes := len(store.propIndexes)
	itemPtr, ok := store.items.Load(id)
	if ok {
		if cntIndexes > 0 {
			oldValues = store.readIndexies(itemPtr)
		}
	}
	store.items.Store(id, data)
	if cntIndexes > 0 {
		store.updateIndexies(data, oldValues)
	}
	logrus.Info("store '"+store.name+"' update: ", id)
}

// Store data store by data
func (store *DataStore) Delete(id string) {
	var itemPtr interface{}
	var oldValues []interface{}
	cntIndexes := len(store.propIndexes)
	itemPtr, ok := store.items.Load(id)
	if ok {
		if cntIndexes > 0 {
			oldValues = store.readIndexies(itemPtr)
		}
		store.items.Delete(id)
		if cntIndexes > 0 {
			store.deleteIndexies(itemPtr, oldValues)
		}
		logrus.Info("store '"+store.name+"' delete: ", id)
	}
}

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
			oldValues = store.readIndexies(itemPtr)
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
			store.updateIndexies(itemPtr, oldValues)
		}
		logrus.Warn("store '"+store.name+"' update: ", id, " ", data)
	}
}

func (store *DataStore) readIndexies(itemPtr interface{}) []interface{} {
	itemVal := reflect.ValueOf(itemPtr)
	if itemVal.Kind() == reflect.Ptr {
		itemVal = reflect.Indirect(itemVal)
	}
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

func (store *DataStore) updateIndexies(itemPtr interface{}, oldValues []interface{}) {
	itemVal := reflect.ValueOf(itemPtr)
	if itemVal.Kind() == reflect.Ptr {
		itemVal = reflect.Indirect(itemVal)
	}
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

func (store *DataStore) deleteIndexies(itemPtr interface{}, oldValues []interface{}) {
	itemVal := reflect.ValueOf(itemPtr)
	if itemVal.Kind() == reflect.Ptr {
		itemVal = reflect.Indirect(itemVal)
	}
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
		val, ok := index.Load(propVal)
		if ok && val == itemPtr {
			index.Delete(propVal)
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
