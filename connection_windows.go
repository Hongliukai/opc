//go:build windows
// +build windows

package opc

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
	"unsafe"

	ole "github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"
)

func init() {
	OleInit()
}

// OleInit initializes OLE.
func OleInit() {
	ole.CoInitializeEx(0, 0)
}

// OleRelease realeses OLE resources in opcAutomation.
func OleRelease() {
	ole.CoUninitialize()
}

// AutomationObject loads the OPC Automation Wrapper and handles to connection to the OPC Server.
type AutomationObject struct {
	unknown *ole.IUnknown
	object  *ole.IDispatch
}

// CreateBrowser returns the OPCBrowser object from the OPCServer.
// It only works if there is a successful connection.
func (ao *AutomationObject) CreateBrowser() (*Tree, error) {
	// check if server is running, if not return error
	if !ao.IsConnected() {
		return nil, errors.New("Cannot create browser because we are not connected.")
	}

	// create browser
	browser, err := oleutil.CallMethod(ao.object, "CreateBrowser")
	if err != nil {
		return nil, errors.New("Failed to create OPCBrowser")
	}

	// move to root
	oleutil.MustCallMethod(browser.ToIDispatch(), "MoveToRoot")

	// create tree
	root := Tree{"root", nil, []*Tree{}, []Leaf{}}
	buildTree(browser.ToIDispatch(), &root)

	return &root, nil
}

// buildTree runs through the OPCBrowser and creates a tree with the OPC tags
func buildTree(browser *ole.IDispatch, branch *Tree) {
	var count int32

	logger.Println("Entering branch:", branch.Name)

	// loop through leafs
	oleutil.MustCallMethod(browser, "ShowLeafs").ToIDispatch()
	count = oleutil.MustGetProperty(browser, "Count").Value().(int32)

	logger.Println("\tLeafs count:", count)

	for i := 1; i <= int(count); i++ {

		item := oleutil.MustCallMethod(browser, "Item", i).Value()
		tag := oleutil.MustCallMethod(browser, "GetItemID", item).Value()

		l := Leaf{Name: item.(string), Tag: tag.(string)}

		logger.Println("\t", i, l)

		branch.Leaves = append(branch.Leaves, l)
	}

	// loop through branches
	oleutil.MustCallMethod(browser, "ShowBranches").ToIDispatch()
	count = oleutil.MustGetProperty(browser, "Count").Value().(int32)

	logger.Println("\tBranches count:", count)

	for i := 1; i <= int(count); i++ {

		nextName := oleutil.MustCallMethod(browser, "Item", i).Value()

		logger.Println("\t", i, "next branch:", nextName)

		// move down
		oleutil.MustCallMethod(browser, "MoveDown", nextName)

		// recursively populate tree
		nextBranch := Tree{nextName.(string), branch, []*Tree{}, []Leaf{}}
		branch.Branches = append(branch.Branches, &nextBranch)
		buildTree(browser, &nextBranch)

		// move up and set branches again
		oleutil.MustCallMethod(browser, "MoveUp")
		oleutil.MustCallMethod(browser, "ShowBranches").ToIDispatch()
	}

	logger.Println("Exiting branch:", branch.Name)

}

// Connect establishes a connection to the OPC Server on node.
// It returns a reference to AutomationItems and error message.
func (ao *AutomationObject) Connect(server string, node string) (*AutomationGroup, *AutomationItems, error) {

	// make sure there is not active connection before trying to connect
	ao.disconnect()

	// try to connect to opc server and check for error
	logger.Printf("Connecting to %s on node %s\n", server, node)
	_, err := oleutil.CallMethod(ao.object, "Connect", server, node)
	if err != nil {
		logger.Println("Connection failed.")
		return nil, nil, errors.New("Connection failed")
	}

	// set up opc groups and items
	opcGroups, err := oleutil.GetProperty(ao.object, "OPCGroups")
	if err != nil {
		//logger.Println(err)
		return nil, nil, errors.New("cannot get OPCGroups property")
	}
	opcGrp, err := oleutil.CallMethod(opcGroups.ToIDispatch(), "Add")
	if err != nil {
		// logger.Println(err)
		return nil, nil, errors.New("cannot add new OPC Group")
	}
	addItemObject, err := oleutil.GetProperty(opcGrp.ToIDispatch(), "OPCItems")
	if err != nil {
		// logger.Println(err)
		return nil, nil, errors.New("cannot get OPC Items")
	}

	opcGroups.ToIDispatch().Release()
	// opcGrp.ToIDispatch().Release()

	logger.Println("Connected.")

	automationItems := NewAutomationItems(addItemObject.ToIDispatch())
	automationGroup := NewAutomationGroup(opcGrp.ToIDispatch(), automationItems)

	return automationGroup, automationItems, nil
}

// TryConnect loops over the nodes array and tries to connect to any of the servers.
func (ao *AutomationObject) TryConnect(server string, nodes []string) (*AutomationGroup, *AutomationItems, error) {
	var errResult string
	for _, node := range nodes {
		group, items, err := ao.Connect(server, node)
		if err == nil {
			return group, items, err
		}
		errResult = errResult + err.Error() + "\n"
	}
	return nil, nil, errors.New("TryConnect was not successful: " + errResult)
}

// IsConnected check if the server is properly connected and up and running.
func (ao *AutomationObject) IsConnected() bool {
	if ao.object == nil {
		return false
	}
	stateVt, err := oleutil.GetProperty(ao.object, "ServerState")
	if err != nil {
		logger.Println("GetProperty call for ServerState failed", err)
		return false
	}
	if stateVt.Value().(int32) != OPCRunning {
		return false
	}
	return true
}

// GetOPCServers returns a list of Prog ID on the specified node
func (ao *AutomationObject) GetOPCServers(node string) []string {
	progids, err := oleutil.CallMethod(ao.object, "GetOPCServers", node)
	if err != nil {
		logger.Println("GetOPCServers call failed.")
		return []string{}
	}

	var servers_found []string
	for _, v := range progids.ToArray().ToStringArray() {
		if v != "" {
			servers_found = append(servers_found, v)
		}
	}
	return servers_found
}

// Disconnect checks if connected to server and if so, it calls 'disconnect'
func (ao *AutomationObject) disconnect() {
	if ao.IsConnected() {
		_, err := oleutil.CallMethod(ao.object, "Disconnect")
		if err != nil {
			logger.Println("Failed to disconnect.")
		}
	}
}

// Close releases the OLE objects in the AutomationObject.
func (ao *AutomationObject) Close() {
	if ao.object != nil {
		ao.disconnect()
		ao.object.Release()
	}
	if ao.unknown != nil {
		ao.unknown.Release()
	}
}

// NewAutomationObject connects to the COM object based on available wrappers.
func NewAutomationObject() *AutomationObject {
	// TODO: list should not be hard-coded
	wrappers := []string{
		"OPC.Automation.1",
		"Graybox.OPC.DAWrapper.1",
		"Matrikon.OPC.Automation.1",
	}
	var err error
	var unknown *ole.IUnknown
	for _, wrapper := range wrappers {
		unknown, err = oleutil.CreateObject(wrapper)
		if err == nil {
			logger.Println("Loaded OPC Automation object with wrapper", wrapper)
			break
		}
		logger.Println("Could not load OPC Automation object with wrapper", wrapper)
	}
	if err != nil {
		return &AutomationObject{}
	}

	opc, err := unknown.QueryInterface(ole.IID_IDispatch)
	if err != nil {
		logger.Println("Could not QueryInterface")
		return &AutomationObject{}
	}
	object := AutomationObject{
		unknown: unknown,
		object:  opc,
	}
	return &object
}

type AutomationGroup struct {
	opcGroup *ole.IDispatch
	*AutomationItems
}

// syncRead reads all items in the AutomationGroup in one call and returns a map of Items.
func (ag *AutomationGroup) syncRead() (map[string]Item, error) {
	count := len(ag.AutomationItems.itemsHandle)
	var serverHandles []int32
	var tags []string = make([]string, 0, count)
	if count == 0 {
		return make(map[string]Item), nil
	}
	if OPCConfig.Mode == ReadModeMultiLowerBound1 {
		// 如果是从1开始读取的模式(VB6 OPCDA)，则需要在数组开头多加一个空元素
		serverHandles = make([]int32, 0, count+1)
		serverHandles = append(serverHandles, 0)
		for tag, serverHandle := range ag.AutomationItems.itemsHandle {
			tags = append(tags, tag)
			serverHandles = append(serverHandles, serverHandle)
		}
	} else if OPCConfig.Mode == ReadModeMultiLowerBound0 {
		// 是从0开始编号的模式
		serverHandles = make([]int32, 0, count)
		for tag, serverHandle := range ag.AutomationItems.itemsHandle {
			tags = append(tags, tag)
			serverHandles = append(serverHandles, serverHandle)
		}
	} else {
		return nil, errors.New("Unsupported ReadMode for sync read")
	}

	var saValues *ole.SafeArray
	var saErrors *ole.SafeArray
	var saQualities *ole.SafeArray
	var saTimestamps *ole.SafeArray

	vValues := ole.NewVariant(ole.VT_ARRAY|ole.VT_VARIANT|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saValues))))
	vErrors := ole.NewVariant(ole.VT_ARRAY|ole.VT_I4|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saErrors))))
	vQualities := ole.NewVariant(ole.VT_ARRAY|ole.VT_VARIANT|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saQualities))))
	vTimestamps := ole.NewVariant(ole.VT_ARRAY|ole.VT_VARIANT|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saTimestamps))))

	logger.Printf("SyncRead %v tags from %v", count, GetReadSourceMeaning(OPCConfig.ReadSource))
	_, err := oleutil.CallMethod(ag.opcGroup, "SyncRead", OPCConfig.ReadSource, count, serverHandles, vValues, vErrors, vQualities, vTimestamps)
	if err != nil {
		return nil, errors.New("Cannot sync read OPC Items")
	}

	var values []interface{}
	var errorCodes []interface{}
	var qualities []interface{}
	var timestamps []interface{}

	valueSac := &ole.SafeArrayConversion{Array: saValues}
	defer valueSac.Release()
	errorSac := &ole.SafeArrayConversion{Array: saErrors}
	defer errorSac.Release()
	qualitySac := &ole.SafeArrayConversion{Array: saQualities}
	defer qualitySac.Release()
	timestampSac := &ole.SafeArrayConversion{Array: saTimestamps}
	defer timestampSac.Release()

	if OPCConfig.Mode == ReadModeMultiLowerBound1 {
		// 如果是从1开始读取的模式(VB6 OPCDA)
		values = valueSac.ToValueArrayWithOffset(1)
		errorCodes = errorSac.ToValueArrayWithOffset(1)
		qualities = qualitySac.ToValueArrayWithOffset(1)
		timestamps = timestampSac.ToValueArrayWithOffset(1)
	}
	if OPCConfig.Mode == ReadModeMultiLowerBound0 {
		// 是从0开始编号的模式(C# OPCDA)
		values = valueSac.ToValueArray()
		errorCodes = errorSac.ToValueArray()
		qualities = qualitySac.ToValueArray()
		timestamps = timestampSac.ToValueArray()
	}

	allTags := make(map[string]Item)
	for i := 0; i < count; i++ {
		if errorCode, ok := errorCodes[i].(int32); ok {
			if errorCode != 0 {
				logger.Printf("Read %v failed, cause: %v", tags[i], GetErrorMessage(errorCode))
				continue
			}
		} else {
			logger.Printf("Read %v failed,, errorCode: %v", tags[i], errorCodes[i])
			continue
		}

		item := Item{
			Value:     values[i],
			Quality:   0,
			Timestamp: time.Now(),
		}
		if i < len(qualities) {
			item.Quality = ensureInt16(qualities[i])
		}
		if i < len(timestamps) {
			item.Timestamp = ensureTimestamp(timestamps[i])
		}
		allTags[tags[i]] = item
	}
	return allTags, nil
}

// syncReadTarget reads only the specified tags in targetTags and returns a map of Items.
func (ag *AutomationGroup) syncReadTarget(targetTags []string) (map[string]Item, error) {
	if len(targetTags) == 0 {
		logger.Println("No valid tags to read.")
		return make(map[string]Item), nil
	}
	var serverHandles []int32
	var tags []string
	for _, tag := range targetTags {
		if _, ok := ag.AutomationItems.itemsHandle[tag]; !ok {
			logger.Printf("Tag %s not found in itemsHandle.", tag)
			continue
		}
		tags = append(tags, tag)
	}
	count := len(tags)
	if count == 0 {
		logger.Println("No valid tags to read.")
		return make(map[string]Item), nil
	}
	if OPCConfig.Mode == ReadModeMultiLowerBound1 {
		// 如果是从1开始读取的模式(VB6 OPCDA)，则需要在数组开头多加一个空元素
		serverHandles = make([]int32, 0, count+1)
		serverHandles = append(serverHandles, 0)
		for _, tag := range tags {
			serverHandles = append(serverHandles, ag.AutomationItems.itemsHandle[tag])
		}
	} else if OPCConfig.Mode == ReadModeMultiLowerBound0 {
		// 是从0开始编号的模式
		serverHandles = make([]int32, 0, count)
		for _, tag := range tags {
			serverHandles = append(serverHandles, ag.AutomationItems.itemsHandle[tag])
		}
	} else {
		return nil, errors.New("Unsupported ReadMode for sync read")
	}

	var saValues *ole.SafeArray
	var saErrors *ole.SafeArray
	var saQualities *ole.SafeArray
	var saTimestamps *ole.SafeArray

	vValues := ole.NewVariant(ole.VT_ARRAY|ole.VT_VARIANT|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saValues))))
	vErrors := ole.NewVariant(ole.VT_ARRAY|ole.VT_I4|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saErrors))))
	vQualities := ole.NewVariant(ole.VT_ARRAY|ole.VT_VARIANT|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saQualities))))
	vTimestamps := ole.NewVariant(ole.VT_ARRAY|ole.VT_VARIANT|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saTimestamps))))

	logger.Printf("SyncRead %v tags from %v", count, GetReadSourceMeaning(OPCConfig.ReadSource))
	_, err := oleutil.CallMethod(ag.opcGroup, "SyncRead", OPCConfig.ReadSource, count, serverHandles, vValues, vErrors, vQualities, vTimestamps)
	if err != nil {
		log.Println("SyncRead failed.", err)
		return nil, errors.New("SyncRead cannot sync read OPC Items")
	}

	var values []interface{}
	var errorCodes []interface{}
	var qualities []interface{}
	var timestamps []interface{}

	valueSac := &ole.SafeArrayConversion{Array: saValues}
	defer valueSac.Release()
	errorSac := &ole.SafeArrayConversion{Array: saErrors}
	defer errorSac.Release()
	qualitySac := &ole.SafeArrayConversion{Array: saQualities}
	defer qualitySac.Release()
	timestampSac := &ole.SafeArrayConversion{Array: saTimestamps}
	defer timestampSac.Release()

	if OPCConfig.Mode == ReadModeMultiLowerBound1 {
		// 如果是从1开始读取的模式(VB6 OPCDA)
		values = valueSac.ToValueArrayWithOffset(1)
		errorCodes = errorSac.ToValueArrayWithOffset(1)
		qualities = qualitySac.ToValueArrayWithOffset(1)
		timestamps = timestampSac.ToValueArrayWithOffset(1)
	}
	if OPCConfig.Mode == ReadModeMultiLowerBound0 {
		// 是从0开始编号的模式(C# OPCDA)
		values = valueSac.ToValueArray()
		errorCodes = errorSac.ToValueArray()
		qualities = qualitySac.ToValueArray()
		timestamps = timestampSac.ToValueArray()
	}

	allTags := make(map[string]Item)
	for i := 0; i < count; i++ {
		if errorCode, ok := errorCodes[i].(int32); ok {
			if errorCode != 0 {
				logger.Printf("SyncRead Read %v failed, cause: %v", tags[i], GetErrorMessage(errorCode))
				continue
			}
		} else {
			logger.Printf("SyncRead Read %v failed, errorCode: %v", tags[i], errorCodes[i])
			continue
		}
		item := Item{
			Value:     values[i],
			Quality:   0,
			Timestamp: time.Now(),
		}
		if i < len(qualities) {
			item.Quality = ensureInt16(qualities[i])
		}
		if i < len(timestamps) {
			item.Timestamp = ensureTimestamp(timestamps[i])
		}
		allTags[tags[i]] = item
	}
	return allTags, nil
}

func (ag *AutomationGroup) syncWriteTarget(tag string, value interface{}) error {
	var serviceHandle int32
	if v, ok := ag.AutomationItems.itemsHandle[tag]; !ok {
		return errors.New("Tag not found in itemsHandle")
	} else {
		serviceHandle = v
	}
	opcItem, err := oleutil.CallMethod(ag.addItemObject, "GetOPCItem", serviceHandle)
	if err != nil {
		return errors.New("Write cannot Get OPC Item")
	}
	err = ag.writeToOpc(opcItem.ToIDispatch(), value)
	if err != nil {
		return err
	}
	return nil
}

// NewAutomationItems returns a new AutomationItems instance.
func NewAutomationGroup(opcGrp *ole.IDispatch, automationItems *AutomationItems) *AutomationGroup {
	ai := AutomationGroup{opcGroup: opcGrp, AutomationItems: automationItems}
	return &ai
}

// AutomationItems store the OPCItems from OPCGroup and does the bookkeeping
// for the individual OPC items. Tags can added, removed, and read.
type AutomationItems struct {
	addItemObject *ole.IDispatch
	items         map[string]*ole.IDispatch
	itemsHandle   map[string]int32
}

// addSingle adds the tag and returns an error. Client handles are not implemented yet.
func (ai *AutomationItems) addSingle(tag string) error {
	clientHandle := int32(1)
	item, err := oleutil.CallMethod(ai.addItemObject, "AddItem", tag, clientHandle)
	if err != nil {
		log.Println("AddItem call failed for tag:", tag, "error:", err)
		return errors.New(tag + ":" + err.Error())
	}
	logger.Println("Added tag:", tag)
	ai.items[tag] = item.ToIDispatch()
	return nil
}

// addMulti adds multiple tags and returns an error.
func (ai *AutomationItems) addMulti(tags []string) error {
	count := len(tags)
	var targetTags []string
	if OPCConfig.Mode == ReadModeMultiLowerBound1 {
		// 如果是从1开始读取的模式(VB6 OPCDA)，则需要在数组开头多加一个""的元素
		targetTags = make([]string, 0, len(tags)+1)
		targetTags = append(targetTags, "")
		targetTags = append(targetTags, tags...)
	} else if OPCConfig.Mode == ReadModeMultiLowerBound0 {
		// 是从0开始编号的模式(C# OPCDA)
		targetTags = make([]string, 0, len(tags))
		targetTags = append(targetTags, tags...)
	} else {
		return errors.New("Unsupported ReadMode for multi add")
	}

	clientHandles := make([]int32, count+1)
	for i := range clientHandles {
		// 我们不使用clientHandles 读取数据，但是也建议从1开始编号，避免一些opc server的兼容性问题
		clientHandles[i] = int32(i + 1)
	}

	var saServerHandles *ole.SafeArray
	var saErrors *ole.SafeArray

	vServerHandles := ole.NewVariant(ole.VT_ARRAY|ole.VT_I4|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saServerHandles))))
	vErrors := ole.NewVariant(ole.VT_ARRAY|ole.VT_I4|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saErrors))))

	_, err := oleutil.CallMethod(ai.addItemObject, "AddItems", count, targetTags, clientHandles, vServerHandles, vErrors)
	if err != nil {
		return fmt.Errorf("Invoke AddItems failed: %v", err)
	}

	sacServer := &ole.SafeArrayConversion{Array: saServerHandles}
	defer sacServer.Release()
	var serverHandles []interface{}

	sacError := &ole.SafeArrayConversion{Array: saErrors}
	defer sacError.Release()
	var errorCodes []interface{}

	if OPCConfig.Mode == ReadModeMultiLowerBound1 {
		serverHandles = sacServer.ToValueArrayWithOffset(1)
		errorCodes = sacError.ToValueArrayWithOffset(1)
	}
	if OPCConfig.Mode == ReadModeMultiLowerBound0 {
		serverHandles = sacServer.ToValueArray()
		errorCodes = sacError.ToValueArray()
	}

	for i := 0; i < count; i++ {
		if errorCode, ok := errorCodes[i].(int32); ok {
			if errorCode != 0 {
				logger.Fatalf("Add Tag %v failed, because %v", tags[i], GetErrorMessage(errorCode))
				continue
			}
		} else {
			logger.Fatalf("Add Tag %v failed, errorCode: %v", tags[i], errorCodes[i])
			continue
		}

		if serverHandle, ok := serverHandles[i].(int32); ok && serverHandle != 0 {
			logger.Printf("Added tag: %v with serverHandle: %v", tags[i], serverHandle)
			ai.itemsHandle[tags[i]] = serverHandle
		} else {
			logger.Fatalf("Add Tag %v failed,Parse serverHandle failed. serverHandle: %v", tags[i], serverHandles[i])
		}
	}

	return nil
}

func (ai *AutomationItems) removeTargetInMultiAddTag(targetTags []string) error {
	var serverHandles []int32
	var targetServerHandles []int32
	for _, tag := range targetTags {
		if v, ok := ai.itemsHandle[tag]; ok {
			targetServerHandles = append(targetServerHandles, v)
		}
	}
	count := len(targetServerHandles)
	if count == 0 {
		logger.Println("No tags to remove.")
		return nil
	}

	if OPCConfig.Mode == ReadModeMultiLowerBound1 {
		// 如果是从1开始读取的模式(VB6 OPCDA)，则需要在数组开头多加一个空元素
		serverHandles = make([]int32, 0, count+1)
		serverHandles = append(serverHandles, 0)
		serverHandles = append(serverHandles, targetServerHandles...)
	} else if OPCConfig.Mode == ReadModeMultiLowerBound0 {
		// 是从0开始编号的模式
		serverHandles = make([]int32, 0, count)
		serverHandles = append(serverHandles, targetServerHandles...)
	} else {
		return errors.New("Unsupported ReadMode")
	}
	var saErrors *ole.SafeArray

	vErrors := ole.NewVariant(ole.VT_ARRAY|ole.VT_I4|ole.VT_BYREF, int64(uintptr(unsafe.Pointer(&saErrors))))

	_, err := oleutil.CallMethod(ai.addItemObject, "Remove", count, serverHandles, vErrors)
	if err != nil {
		return errors.New("cannot remove OPC Items")
	}
	var errorCodes []interface{}
	errorSac := &ole.SafeArrayConversion{Array: saErrors}
	defer errorSac.Release()
	if OPCConfig.Mode == ReadModeMultiLowerBound1 {
		// 如果是从1开始读取的模式(VB6 OPCDA)
		errorCodes = errorSac.ToValueArrayWithOffset(1)
	}
	if OPCConfig.Mode == ReadModeMultiLowerBound0 {
		// 是从0开始编号的模式(C# OPCDA)
		errorCodes = errorSac.ToValueArray()
	}
	for i := 0; i < count; i++ {
		if errorCode, ok := errorCodes[i].(int32); ok {
			if errorCode != 0 {
				logger.Printf("remove %v failed, cause: %v", targetTags[i], GetErrorMessage(errorCode))
			} else {
				logger.Printf("removed %v successfully,code: %v", targetTags[i], errorCode)
			}
			continue
		} else {
			logger.Printf("remove %v failed, errorCode: %v", targetTags[i], errorCodes[i])
			continue
		}
	}
	return nil
}

// Add accepts a variadic parameters of tags.
func (ai *AutomationItems) Add(tags ...string) error {
	if len(tags) == 0 {
		logger.Println("No tags to add.")
		return nil
	}
	if OPCConfig.Mode == ReadModeSingle {
		var errResult string
		for _, tag := range tags {
			err := ai.addSingle(tag)
			if err != nil {
				errResult = err.Error() + errResult
			}
		}
		if errResult != "" {
			return errors.New(errResult)
		}
		logger.Println("ReadModeSingle Mode Add Items Complete ,Print All:")
		for v, _ := range ai.items {
			logger.Println("\t", v)
		}
		return nil
	}
	err := ai.addMulti(tags)
	if err != nil {
		return err
	}
	logger.Println("ReadModeMulti Mode Add Items Complete ,Print All:")
	for k, v := range ai.itemsHandle {
		logger.Printf("\t tag: %v handler:%v\n", k, v)
	}
	return nil

}

// Remove removes the tag.
func (ai *AutomationItems) Remove(tag string) {
	logger.Printf("Removing tag %s", tag)
	if OPCConfig.Mode == ReadModeSingle {
		item, ok := ai.items[tag]
		if ok {
			item.Release()
			delete(ai.items, tag)
		}
		return
	}
	_, ok := ai.itemsHandle[tag]
	if ok {
		ai.removeTargetInMultiAddTag([]string{tag})
		delete(ai.itemsHandle, tag)
	}
}

/*
 * FIX:
 * some opc servers sometimes returns an int32 Quality, that produces panic
 */
func ensureInt16(q interface{}) int16 {
	if v16, ok := q.(int16); ok {
		return v16
	}
	if v32, ok := q.(int32); ok && v32 >= -32768 && v32 < 32768 {
		return int16(v32)
	}
	return 0
}

/*
 * FIX:
 * some opc servers sometimes returns an int32 Quality, that produces panic
 */
func ensureTimestamp(q interface{}) time.Time {
	if t, ok := q.(time.Time); ok {
		return t
	}
	return time.Now()
}

// readFromOPC reads from the server and returns an Item and error.
func (ai *AutomationItems) readFromOpc(opcitem *ole.IDispatch) (Item, error) {
	v := ole.NewVariant(ole.VT_R4, 0)
	q := ole.NewVariant(ole.VT_INT, 0)
	ts := ole.NewVariant(ole.VT_DATE, 0)

	//read tag from opc server and monitor duration in seconds
	t := time.Now()
	_, err := oleutil.CallMethod(opcitem, "Read", OPCConfig.ReadSource, &v, &q, &ts)
	opcReadsDuration.Observe(time.Since(t).Seconds())

	if err != nil {
		opcReadsCounter.WithLabelValues("failed").Inc()
		return Item{}, err
	}
	opcReadsCounter.WithLabelValues("success").Inc()

	return Item{
		Value:     v.Value(),
		Quality:   ensureInt16(q.Value()), // FIX: ensure the quality value is int16
		Timestamp: ts.Value().(time.Time),
	}, nil
}

// writeToOPC writes value to opc tag and return an error
func (ai *AutomationItems) writeToOpc(opcitem *ole.IDispatch, value interface{}) error {
	_, err := oleutil.CallMethod(opcitem, "Write", value)
	if err != nil {
		// TODO: Prometheus Monitoring
		//opcWritesCounter.WithLabelValues("failed").Inc()
		return err
	}
	//opcWritesCounter.WithLabelValues("failed").Inc()
	return nil
}

// Close closes the OLE objects in AutomationItems.
func (ai *AutomationItems) Close() {
	logger.Println("Releasing AutomationItems resources.")
	if ai != nil {
		for key, opcitem := range ai.items {
			opcitem.Release()
			delete(ai.items, key)
		}
		tags := make([]string, 0, len(ai.itemsHandle))
		for tag, _ := range ai.itemsHandle {
			tags = append(tags, tag)
		}
		ai.removeTargetInMultiAddTag(tags)
		ai.addItemObject.Release()
	}
}

// NewAutomationItems returns a new AutomationItems instance.
func NewAutomationItems(opcitems *ole.IDispatch) *AutomationItems {
	ai := AutomationItems{addItemObject: opcitems, items: make(map[string]*ole.IDispatch), itemsHandle: make(map[string]int32)}
	return &ai
}

// opcRealServer implements the Connection interface.
// It has the AutomationObject embedded for connecting to the server
// and an AutomationItems to facilitate the OPC items bookkeeping.
type opcConnectionImpl struct {
	*AutomationObject
	*AutomationGroup
	*AutomationItems
	Server string
	Nodes  []string
	mu     sync.Mutex
}

// ReadItem returns an Item for a specific tag.
func (conn *opcConnectionImpl) ReadItem(tag string) Item {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if OPCConfig.Mode == ReadModeSingle {
		opcitem, ok := conn.AutomationItems.items[tag]
		if ok {
			item, err := conn.AutomationItems.readFromOpc(opcitem)
			if err == nil {
				return item
			}
			logger.Printf("Cannot read %s: %s. Trying to fix.", tag, err)
			conn.fix()
		} else {
			logger.Printf("Tag %s not found. Add it first before reading it.", tag)
		}
		return Item{}
	}
	_, ok := conn.AutomationItems.itemsHandle[tag]
	if ok {
		items, err := conn.AutomationGroup.syncReadTarget([]string{tag})
		if err == nil {
			return items[tag]
		}
		logger.Printf("Cannot read %s: %s. Trying to fix.", tag, err)
		conn.fix()
	} else {
		logger.Printf("Tag %s not found. Add it first before reading it.", tag)
	}

	return Item{}
}

// Write writes a value to the OPC Server.
func (conn *opcConnectionImpl) Write(tag string, value interface{}) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	logger.Printf("Writing value %v to tag %s", value, tag)
	if OPCConfig.Mode == ReadModeSingle {
		opcitem, ok := conn.AutomationItems.items[tag]
		if ok {
			return conn.AutomationItems.writeToOpc(opcitem, value)
		}
		logger.Printf("Tag %s not found. Add it first before writing to it.", tag)
		return errors.New("No Write performed")
	}
	_, ok := conn.AutomationItems.itemsHandle[tag]
	if ok {
		return conn.AutomationGroup.syncWriteTarget(tag, value)
	}
	logger.Printf("Tag %s not found. Add it first before writing to it.", tag)
	return errors.New("No Write performed")
}

// Read returns a map of the values of all added tags.
func (conn *opcConnectionImpl) Read() map[string]Item {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if OPCConfig.Mode == ReadModeSingle {
		allTags := make(map[string]Item)
		for tag, opcitem := range conn.AutomationItems.items {
			item, err := conn.AutomationItems.readFromOpc(opcitem)
			if err != nil {
				logger.Printf("Cannot read %s: %s. Trying to fix.", tag, err)
				conn.fix()
				break
			}
			allTags[tag] = item
		}
		return allTags
	}
	data, err := conn.AutomationGroup.syncRead()
	if err != nil {
		logger.Printf("Errors during sync read: %v. Trying to fix.", err)
		conn.fix()
	}
	return data
}

func (conn *opcConnectionImpl) ReadItems(tags ...string) map[string]Item {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if OPCConfig.Mode == ReadModeSingle {
		allTags := make(map[string]Item)
		for _, tag := range tags {
			opcitem, ok := conn.AutomationItems.items[tag]
			if ok {
				item, err := conn.AutomationItems.readFromOpc(opcitem)
				if err != nil {
					logger.Printf("Cannot read %s: %s. Trying to fix.", tag, err)
					conn.fix()
					break
				}
				allTags[tag] = item
			} else {
				logger.Printf("Tag %s not found. Add it first before reading it.", tag)
				break
			}
		}
		return allTags
	}
	data, err := conn.AutomationGroup.syncReadTarget(tags)
	if err != nil {
		logger.Printf("Errors during sync read: %v. Trying to fix.", err)
		conn.fix()
	}
	return data
}

// Tags returns the currently active tags
func (conn *opcConnectionImpl) Tags() []string {
	var tags []string
	if conn.AutomationItems != nil {
		for tag, _ := range conn.AutomationItems.items {
			tags = append(tags, tag)
		}
		for tag, _ := range conn.AutomationItems.itemsHandle {
			tags = append(tags, tag)
		}
	}
	return tags

}

// fix tries to reconnect if connection is lost by creating a new connection
// with AutomationObject and creating a new AutomationItems instance.
func (conn *opcConnectionImpl) fix() {
	var err error
	if !conn.IsConnected() {
		for {
			tags := conn.Tags()
			conn.AutomationItems.Close()
			conn.AutomationGroup, conn.AutomationItems, err = conn.TryConnect(conn.Server, conn.Nodes)
			if err != nil {
				logger.Println(err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			tagErr := conn.Add(tags...)
			if tagErr != nil {
				logger.Printf("fix Connection:Failed to add tags after reconnecting: %v", tagErr)
			}
			break
		}
	}
}

// Close closes the embedded types.
func (conn *opcConnectionImpl) Close() {
	conn.mu.Lock()
	logger.Println("Cleaning OPC connection.")
	defer conn.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		// must  close items first
		if conn.AutomationItems != nil {
			conn.AutomationItems.Close()
		}
		if conn.AutomationObject != nil {
			conn.AutomationObject.Close()
		}
		logger.Println("OPC connection closed.")
	}()

	select {
	case <-done:
		// closed successfully
	case <-time.After(10 * time.Second):
		logger.Println("Timeout while closing OPC connection.")
	}

}

// NewConnection establishes a connection to the OpcServer object.
func NewConnection(server string, nodes []string, tags []string) (Connection, error) {
	object := NewAutomationObject()
	group, items, err := object.TryConnect(server, nodes)
	if err != nil {
		return &opcConnectionImpl{}, err
	}
	logger.Printf("Request to add tags:")
	err = items.Add(tags...)
	if err != nil {
		logger.Println("Adding tags failed:", err)
		return &opcConnectionImpl{}, err
	}
	conn := opcConnectionImpl{
		AutomationObject: object,
		AutomationGroup:  group,
		AutomationItems:  items,
		Server:           server,
		Nodes:            nodes,
	}

	return &conn, nil
}

// CreateBrowser creates an opc browser representation
func CreateBrowser(server string, nodes []string) (*Tree, error) {
	object := NewAutomationObject()
	defer object.Close()
	_, _, err := object.TryConnect(server, nodes)
	if err != nil {
		return nil, err
	}
	return object.CreateBrowser()
}
