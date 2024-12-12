package opencost

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/opencost/opencost/core/pkg/util/json"
)

// Encoding and decoding logic for Asset types

// Any marshal and unmarshal

// MarshalJSON implements json.Marshaler
func (a *Any) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncode(buffer, "properties", a.Properties, ",")
	jsonEncode(buffer, "labels", a.Labels, ",")
	jsonEncode(buffer, "window", a.Window, ",")
	jsonEncodeString(buffer, "start", a.Start.Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", a.End.Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", a.Minutes(), ",")
	jsonEncodeFloat64(buffer, "adjustment", a.Adjustment, ",")
	jsonEncodeFloat64(buffer, "totalCost", a.TotalCost(), "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

func (a *Any) UnmarshalJSON(b []byte) error {

	var f interface{}

	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	err = a.InterfaceToAny(f)
	if err != nil {
		return err
	}

	return nil
}

// Converts interface{} to Any, carrying over relevant fields
func (a *Any) InterfaceToAny(itf interface{}) error {

	fmap := itf.(map[string]interface{})

	// parse properties map to AssetProperties
	fproperties := fmap["properties"].(map[string]interface{})
	properties := toAssetProp(fproperties)

	// parse labels map to AssetLabels
	labels := make(map[string]string)
	for k, v := range fmap["labels"].(map[string]interface{}) {
		labels[k] = v.(string)
	}

	// parse start and end strings to time.Time
	start, err := time.Parse(time.RFC3339, fmap["start"].(string))
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339, fmap["end"].(string))
	if err != nil {
		return err
	}

	a.Properties = &properties
	a.Labels = labels
	a.Start = start
	a.End = end
	a.Window = Window{
		start: &start,
		end:   &end,
	}

	if adjustment, err := getTypedVal(fmap["adjustment"]); err == nil {
		a.Adjustment = adjustment.(float64)
	}
	if Cost, err := getTypedVal(fmap["totalCost"]); err == nil {
		a.Cost = Cost.(float64) - a.Adjustment
	}

	return nil
}

// Cloud marshal and unmarshal

// MarshalJSON implements json.Marshaler
func (ca *Cloud) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncodeString(buffer, "type", ca.Type().String(), ",")
	jsonEncode(buffer, "properties", ca.Properties, ",")
	jsonEncode(buffer, "labels", ca.Labels, ",")
	jsonEncode(buffer, "window", ca.Window, ",")
	jsonEncodeString(buffer, "start", ca.Start.Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", ca.End.Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", ca.Minutes(), ",")
	jsonEncodeFloat64(buffer, "adjustment", ca.Adjustment, ",")
	jsonEncodeFloat64(buffer, "credit", ca.Credit, ",")
	jsonEncodeFloat64(buffer, "totalCost", ca.TotalCost(), "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

func (ca *Cloud) UnmarshalJSON(b []byte) error {

	var f interface{}

	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	err = ca.InterfaceToCloud(f)
	if err != nil {
		return err
	}

	return nil
}

// Converts interface{} to Cloud, carrying over relevant fields
func (ca *Cloud) InterfaceToCloud(itf interface{}) error {

	fmap := itf.(map[string]interface{})

	// parse properties map to AssetProperties
	fproperties := fmap["properties"].(map[string]interface{})
	properties := toAssetProp(fproperties)

	// parse labels map to AssetLabels
	labels := make(map[string]string)
	for k, v := range fmap["labels"].(map[string]interface{}) {
		labels[k] = v.(string)
	}

	// parse start and end strings to time.Time
	start, err := time.Parse(time.RFC3339, fmap["start"].(string))
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339, fmap["end"].(string))
	if err != nil {
		return err
	}

	ca.Properties = &properties
	ca.Labels = labels
	ca.Start = start
	ca.End = end
	ca.Window = Window{
		start: &start,
		end:   &end,
	}

	if adjustment, err := getTypedVal(fmap["adjustment"]); err == nil {
		ca.Adjustment = adjustment.(float64)
	}
	if Credit, err := getTypedVal(fmap["credit"]); err == nil {
		ca.Credit = Credit.(float64)
	}
	if Cost, err := getTypedVal(fmap["totalCost"]); err == nil {
		ca.Cost = Cost.(float64) - ca.Adjustment - ca.Credit
	}

	return nil
}

// ClusterManagement marshal and unmarshal

// MarshalJSON implements json.Marshler
func (cm *ClusterManagement) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncodeString(buffer, "type", cm.Type().String(), ",")
	jsonEncode(buffer, "properties", cm.Properties, ",")
	jsonEncode(buffer, "labels", cm.Labels, ",")
	jsonEncode(buffer, "window", cm.Window, ",")
	jsonEncodeString(buffer, "start", cm.GetStart().Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", cm.GetEnd().Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", cm.Minutes(), ",")
	jsonEncodeFloat64(buffer, "totalCost", cm.TotalCost(), "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

func (cm *ClusterManagement) UnmarshalJSON(b []byte) error {

	var f interface{}

	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	err = cm.InterfaceToClusterManagement(f)
	if err != nil {
		return err
	}

	return nil
}

// Converts interface{} to ClusterManagement, carrying over relevant fields
func (cm *ClusterManagement) InterfaceToClusterManagement(itf interface{}) error {

	fmap := itf.(map[string]interface{})

	// parse properties map to AssetProperties
	fproperties := fmap["properties"].(map[string]interface{})
	properties := toAssetProp(fproperties)

	// parse labels map to AssetLabels
	labels := make(map[string]string)
	for k, v := range fmap["labels"].(map[string]interface{}) {
		labels[k] = v.(string)
	}

	// parse start and end strings to time.Time
	start, err := time.Parse(time.RFC3339, fmap["start"].(string))
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339, fmap["end"].(string))
	if err != nil {
		return err
	}

	cm.Properties = &properties
	cm.Labels = labels
	cm.Window = Window{
		start: &start,
		end:   &end,
	}

	if Cost, err := getTypedVal(fmap["totalCost"]); err == nil {
		cm.Cost = Cost.(float64)
	}

	return nil
}

// Disk marshal and unmarshal

// MarshalJSON implements the json.Marshaler interface
func (d *Disk) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncodeString(buffer, "type", d.Type().String(), ",")
	jsonEncode(buffer, "properties", d.Properties, ",")
	jsonEncode(buffer, "labels", d.Labels, ",")
	jsonEncode(buffer, "window", d.Window, ",")
	jsonEncodeString(buffer, "start", d.Start.Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", d.End.Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", d.Minutes(), ",")
	jsonEncodeFloat64(buffer, "byteHours", d.ByteHours, ",")
	jsonEncodeFloat64(buffer, "bytes", d.Bytes(), ",")
	if d.ByteHoursUsed == nil {
		jsonEncode(buffer, "byteHoursUsed", nil, ",")
	} else {
		jsonEncodeFloat64(buffer, "byteHoursUsed", *d.ByteHoursUsed, ",")
	}
	if d.ByteUsageMax == nil {
		jsonEncode(buffer, "byteUsageMax", nil, ",")
	} else {
		jsonEncodeFloat64(buffer, "byteUsageMax", *d.ByteUsageMax, ",")
	}
	jsonEncode(buffer, "breakdown", d.Breakdown, ",")
	jsonEncodeFloat64(buffer, "adjustment", d.Adjustment, ",")
	jsonEncodeFloat64(buffer, "totalCost", d.TotalCost(), ",")
	jsonEncodeString(buffer, "storageClass", d.StorageClass, ",")
	jsonEncodeString(buffer, "volumeName", d.VolumeName, ",")
	jsonEncodeString(buffer, "claimName", d.ClaimName, ",")
	jsonEncodeFloat64(buffer, "local", d.Local, ",")
	jsonEncodeString(buffer, "claimNamespace", d.ClaimNamespace, "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

func (d *Disk) UnmarshalJSON(b []byte) error {

	var f interface{}

	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	err = d.InterfaceToDisk(f)
	if err != nil {
		return err
	}

	return nil
}

// Converts interface{} to Disk, carrying over relevant fields
func (d *Disk) InterfaceToDisk(itf interface{}) error {

	fmap := itf.(map[string]interface{})

	// parse properties map to AssetProperties
	fproperties := fmap["properties"].(map[string]interface{})
	properties := toAssetProp(fproperties)

	// parse labels map to AssetLabels
	labels := make(map[string]string)
	for k, v := range fmap["labels"].(map[string]interface{}) {
		labels[k] = v.(string)
	}

	// parse start and end strings to time.Time
	start, err := time.Parse(time.RFC3339, fmap["start"].(string))
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339, fmap["end"].(string))
	if err != nil {
		return err
	}

	fbreakdown := fmap["breakdown"].(map[string]interface{})

	breakdown := toBreakdown(fbreakdown)

	d.Properties = &properties
	d.Labels = labels
	d.Start = start
	d.End = end
	d.Window = Window{
		start: &start,
		end:   &end,
	}
	d.Breakdown = &breakdown

	if adjustment, err := getTypedVal(fmap["adjustment"]); err == nil {
		d.Adjustment = adjustment.(float64)
	}
	if Cost, err := getTypedVal(fmap["totalCost"]); err == nil {
		d.Cost = Cost.(float64) - d.Adjustment
	}
	if ByteHours, err := getTypedVal(fmap["byteHours"]); err == nil {
		d.ByteHours = ByteHours.(float64)
	}
	if ByteHoursUsed, err := getTypedVal(fmap["byteHoursUsed"]); err == nil {
		if ByteHoursUsed == nil {
			d.ByteHoursUsed = nil
		} else {
			byteHours := ByteHoursUsed.(float64)
			d.ByteHoursUsed = &byteHours
		}
	}
	if ByteUsageMax, err := getTypedVal(fmap["byteUsageMax"]); err == nil {
		if ByteUsageMax == nil {
			d.ByteUsageMax = nil
		} else {
			max := ByteUsageMax.(float64)
			d.ByteUsageMax = &max
		}
	}

	if StorageClass, err := getTypedVal(fmap["storageClass"]); err == nil {
		d.StorageClass = StorageClass.(string)
	}
	if VolumeName, err := getTypedVal(fmap["volumeName"]); err == nil {
		d.VolumeName = VolumeName.(string)
	}
	if ClaimName, err := getTypedVal(fmap["claimName"]); err == nil {
		d.ClaimName = ClaimName.(string)
	}
	if ClaimNamespace, err := getTypedVal(fmap["claimNamespace"]); err == nil {
		d.ClaimNamespace = ClaimNamespace.(string)
	}

	if local, err := getTypedVal(fmap["local"]); err == nil {
		d.Local = local.(float64)
	}

	return nil

}

// Network marshal and unmarshal

// MarshalJSON implements json.Marshal interface
func (n *Network) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncodeString(buffer, "type", n.Type().String(), ",")
	jsonEncode(buffer, "properties", n.Properties, ",")
	jsonEncode(buffer, "labels", n.Labels, ",")
	jsonEncode(buffer, "window", n.Window, ",")
	jsonEncodeString(buffer, "start", n.Start.Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", n.End.Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", n.Minutes(), ",")
	jsonEncodeFloat64(buffer, "adjustment", n.Adjustment, ",")
	jsonEncodeFloat64(buffer, "totalCost", n.TotalCost(), "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

func (n *Network) UnmarshalJSON(b []byte) error {

	var f interface{}

	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	err = n.InterfaceToNetwork(f)
	if err != nil {
		return err
	}

	return nil
}

// Converts interface{} to Network, carrying over relevant fields
func (n *Network) InterfaceToNetwork(itf interface{}) error {

	fmap := itf.(map[string]interface{})

	// parse properties map to AssetProperties
	fproperties := fmap["properties"].(map[string]interface{})
	properties := toAssetProp(fproperties)

	// parse labels map to AssetLabels
	labels := make(map[string]string)
	for k, v := range fmap["labels"].(map[string]interface{}) {
		labels[k] = v.(string)
	}

	// parse start and end strings to time.Time
	start, err := time.Parse(time.RFC3339, fmap["start"].(string))
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339, fmap["end"].(string))
	if err != nil {
		return err
	}

	n.Properties = &properties
	n.Labels = labels
	n.Start = start
	n.End = end
	n.Window = Window{
		start: &start,
		end:   &end,
	}

	if adjustment, err := getTypedVal(fmap["adjustment"]); err == nil {
		n.Adjustment = adjustment.(float64)
	}
	if Cost, err := getTypedVal(fmap["totalCost"]); err == nil {
		n.Cost = Cost.(float64) - n.Adjustment
	}

	return nil

}

// Node marshal and unmarshal

// MarshalJSON implements json.Marshal interface
func (n *Node) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncodeString(buffer, "type", n.Type().String(), ",")
	jsonEncode(buffer, "properties", n.Properties, ",")
	jsonEncode(buffer, "labels", n.Labels, ",")
	jsonEncode(buffer, "window", n.Window, ",")
	jsonEncodeString(buffer, "start", n.Start.Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", n.End.Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", n.Minutes(), ",")
	jsonEncodeString(buffer, "nodeType", n.NodeType, ",")
	if poolName := GetNodePoolName(n.Properties.Provider, n.Labels); poolName != "" {
		jsonEncodeString(buffer, "pool", poolName, ",")
	}
	jsonEncodeFloat64(buffer, "cpuCores", n.CPUCores(), ",")
	jsonEncodeFloat64(buffer, "ramBytes", n.RAMBytes(), ",")
	jsonEncodeFloat64(buffer, "cpuCoreHours", n.CPUCoreHours, ",")
	jsonEncodeFloat64(buffer, "ramByteHours", n.RAMByteHours, ",")
	jsonEncodeFloat64(buffer, "GPUHours", n.GPUHours, ",")
	jsonEncode(buffer, "cpuBreakdown", n.CPUBreakdown, ",")
	jsonEncode(buffer, "ramBreakdown", n.RAMBreakdown, ",")
	jsonEncodeFloat64(buffer, "preemptible", n.Preemptible, ",")
	jsonEncodeFloat64(buffer, "discount", n.Discount, ",")
	jsonEncodeFloat64(buffer, "cpuCost", n.CPUCost, ",")
	jsonEncodeFloat64(buffer, "gpuCost", n.GPUCost, ",")
	jsonEncodeFloat64(buffer, "gpuCount", n.GPUs(), ",")
	jsonEncodeFloat64(buffer, "ramCost", n.RAMCost, ",")
	jsonEncodeFloat64(buffer, "adjustment", n.Adjustment, ",")
	if n.Overhead != nil {
		jsonEncode(buffer, "overhead", n.Overhead, ",")
	}
	jsonEncodeFloat64(buffer, "totalCost", n.TotalCost(), "")

	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

func (n *Node) UnmarshalJSON(b []byte) error {

	var f interface{}

	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	err = n.InterfaceToNode(f)
	if err != nil {
		return err
	}

	return nil
}

// Converts interface{} to Node, carrying over relevant fields
func (n *Node) InterfaceToNode(itf interface{}) error {

	fmap := itf.(map[string]interface{})

	// parse properties map to AssetProperties
	fproperties := fmap["properties"].(map[string]interface{})
	properties := toAssetProp(fproperties)

	// parse labels map to AssetLabels
	labels := make(map[string]string)
	if labelsInterface, ok := fmap["labels"].(map[string]interface{}); ok {
		for k, v := range labelsInterface {
			if strValue, ok := v.(string); ok {
				labels[k] = strValue
			}
		}
	}

	// parse start and end strings to time.Time
	start, err := time.Parse(time.RFC3339, fmap["start"].(string))
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339, fmap["end"].(string))
	if err != nil {
		return err
	}

	fcpuBreakdown := fmap["cpuBreakdown"].(map[string]interface{})
	framBreakdown := fmap["ramBreakdown"].(map[string]interface{})

	cpuBreakdown := toBreakdown(fcpuBreakdown)
	ramBreakdown := toBreakdown(framBreakdown)

	n.Properties = &properties
	n.Labels = labels
	n.Start = start
	n.End = end
	n.Window = Window{
		start: &start,
		end:   &end,
	}
	n.CPUBreakdown = &cpuBreakdown
	n.RAMBreakdown = &ramBreakdown

	if adjustment, err := getTypedVal(fmap["adjustment"]); err == nil {
		n.Adjustment = adjustment.(float64)
	}
	if NodeType, err := getTypedVal(fmap["nodeType"]); err == nil {
		n.NodeType = NodeType.(string)
	}

	if CPUCoreHours, err := getTypedVal(fmap["cpuCoreHours"]); err == nil {
		n.CPUCoreHours = CPUCoreHours.(float64)
	}
	if RAMByteHours, err := getTypedVal(fmap["ramByteHours"]); err == nil {
		n.RAMByteHours = RAMByteHours.(float64)
	}
	if GPUHours, err := getTypedVal(fmap["GPUHours"]); err == nil {
		n.GPUHours = GPUHours.(float64)
	}
	if CPUCost, err := getTypedVal(fmap["cpuCost"]); err == nil {
		n.CPUCost = CPUCost.(float64)
	}
	if GPUCost, err := getTypedVal(fmap["gpuCost"]); err == nil {
		n.GPUCost = GPUCost.(float64)
	}
	if GPUCount, err := getTypedVal(fmap["gpuCount"]); err == nil {
		n.GPUCount = GPUCount.(float64)
	}
	if RAMCost, err := getTypedVal(fmap["ramCost"]); err == nil {
		n.RAMCost = RAMCost.(float64)
	}
	if Discount, err := getTypedVal(fmap["discount"]); err == nil {
		n.Discount = Discount.(float64)
	}
	if Preemptible, err := getTypedVal(fmap["preemptible"]); err == nil {
		n.Preemptible = Preemptible.(float64)
	}

	return nil
}

// Loadbalancer marshal and unmarshal

// MarshalJSON implements json.Marshal
func (lb *LoadBalancer) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncodeString(buffer, "type", lb.Type().String(), ",")
	jsonEncode(buffer, "properties", lb.Properties, ",")
	jsonEncode(buffer, "labels", lb.Labels, ",")
	jsonEncode(buffer, "window", lb.Window, ",")
	jsonEncodeString(buffer, "start", lb.Start.Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", lb.End.Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", lb.Minutes(), ",")
	jsonEncodeFloat64(buffer, "adjustment", lb.Adjustment, ",")
	jsonEncodeFloat64(buffer, "totalCost", lb.TotalCost(), ",")
	jsonEncode(buffer, "private", lb.Private, ",")
	jsonEncodeString(buffer, "ip", lb.Ip, "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

func (lb *LoadBalancer) UnmarshalJSON(b []byte) error {

	var f interface{}

	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	err = lb.InterfaceToLoadBalancer(f)
	if err != nil {
		return err
	}

	return nil
}

// Converts interface{} to LoadBalancer, carrying over relevant fields
func (lb *LoadBalancer) InterfaceToLoadBalancer(itf interface{}) error {

	fmap := itf.(map[string]interface{})

	// parse properties map to AssetProperties
	fproperties := fmap["properties"].(map[string]interface{})
	properties := toAssetProp(fproperties)

	// parse labels map to AssetLabels
	labels := make(map[string]string)
	for k, v := range fmap["labels"].(map[string]interface{}) {
		labels[k] = v.(string)
	}

	// parse start and end strings to time.Time
	start, err := time.Parse(time.RFC3339, fmap["start"].(string))
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339, fmap["end"].(string))
	if err != nil {
		return err
	}

	lb.Properties = &properties
	lb.Labels = labels
	lb.Start = start
	lb.End = end
	lb.Window = Window{
		start: &start,
		end:   &end,
	}

	if adjustment, err := getTypedVal(fmap["adjustment"]); err == nil {
		lb.Adjustment = adjustment.(float64)
	}
	if Cost, err := getTypedVal(fmap["totalCost"]); err == nil {
		lb.Cost = Cost.(float64) - lb.Adjustment
	}
	if private, err := getTypedVal(fmap["private"]); err == nil {
		lb.Private = private.(bool)
	}
	if ip, err := getTypedVal(fmap["ip"]); err == nil {
		lb.Ip = ip.(string)
	}

	return nil

}

// SharedAsset marshal and unmarshal

// MarshalJSON implements json.Marshaler
func (sa *SharedAsset) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncodeString(buffer, "type", sa.Type().String(), ",")
	jsonEncode(buffer, "properties", sa.Properties, ",")
	jsonEncode(buffer, "labels", sa.Labels, ",")
	jsonEncode(buffer, "window", sa.Window, ",")
	jsonEncodeString(buffer, "start", sa.GetStart().Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", sa.GetEnd().Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", sa.Minutes(), ",")
	jsonEncodeFloat64(buffer, "totalCost", sa.TotalCost(), "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

func (sa *SharedAsset) UnmarshalJSON(b []byte) error {

	var f interface{}

	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	err = sa.InterfaceToSharedAsset(f)
	if err != nil {
		return err
	}

	return nil
}

// Converts interface{} to SharedAsset, carrying over relevant fields
func (sa *SharedAsset) InterfaceToSharedAsset(itf interface{}) error {

	fmap := itf.(map[string]interface{})

	// parse properties map to AssetProperties
	fproperties := fmap["properties"].(map[string]interface{})
	properties := toAssetProp(fproperties)

	// parse labels map to AssetLabels
	labels := make(map[string]string)
	for k, v := range fmap["labels"].(map[string]interface{}) {
		labels[k] = v.(string)
	}

	// parse start and end strings to time.Time
	start, err := time.Parse(time.RFC3339, fmap["start"].(string))
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339, fmap["end"].(string))
	if err != nil {
		return err
	}

	sa.Properties = &properties
	sa.Labels = labels
	sa.Window = Window{
		start: &start,
		end:   &end,
	}

	if Cost, err := getTypedVal(fmap["totalCost"]); err == nil {
		sa.Cost = Cost.(float64)
	}

	return nil

}

// AssetSet marshal

// MarshalJSON JSON-encodes the AssetSet
func (as *AssetSet) MarshalJSON() ([]byte, error) {
	if as == nil {
		return json.Marshal(map[string]Asset{})
	}

	return json.Marshal(as.Assets)
}

// AssetSetResponse for unmarshaling of AssetSet.assets into AssetSet

// Unmarshals a marshaled AssetSet json into AssetSetResponse
func (asr *AssetSetResponse) UnmarshalJSON(b []byte) error {
	var assetMap map[string]*json.RawMessage

	// Partial unmarshal to map of json RawMessage
	err := json.Unmarshal(b, &assetMap)
	if err != nil {
		return err
	}

	err = asr.RawMessageToAssetSetResponse(assetMap)
	if err != nil {
		return err
	}

	return nil
}

func (asr *AssetSetResponse) RawMessageToAssetSetResponse(assetMap map[string]*json.RawMessage) error {

	newAssetMap := make(map[string]Asset)

	// For each item in asset map, unmarshal to appropriate type
	for key, rawMessage := range assetMap {

		var f interface{}

		err := json.Unmarshal(*rawMessage, &f)
		if err != nil {
			return err
		}

		fmap := f.(map[string]interface{})

		switch t := fmap["type"]; t {
		case "Cloud":

			var ca Cloud
			err := ca.InterfaceToCloud(f)

			if err != nil {
				return err
			}

			newAssetMap[key] = &ca

		case "ClusterManagement":

			var cm ClusterManagement
			err := cm.InterfaceToClusterManagement(f)

			if err != nil {
				return err
			}

			newAssetMap[key] = &cm

		case "Disk":

			var d Disk
			err := d.InterfaceToDisk(f)

			if err != nil {
				return err
			}

			newAssetMap[key] = &d

		case "Network":

			var nw Network
			err := nw.InterfaceToNetwork(f)

			if err != nil {
				return err
			}

			newAssetMap[key] = &nw

		case "Node":

			var n Node
			err := n.InterfaceToNode(f)

			if err != nil {
				return err
			}

			newAssetMap[key] = &n

		case "LoadBalancer":

			var lb LoadBalancer
			err := lb.InterfaceToLoadBalancer(f)

			if err != nil {
				return err
			}

			newAssetMap[key] = &lb

		case "Shared":

			var sa SharedAsset
			err := sa.InterfaceToSharedAsset(f)

			if err != nil {
				return err
			}

			newAssetMap[key] = &sa

		default:

			var a Any
			err := a.InterfaceToAny(f)

			if err != nil {
				return err
			}

			newAssetMap[key] = &a

		}
	}

	asr.Assets = newAssetMap

	return nil
}

func (asrr *AssetSetRangeResponse) UnmarshalJSON(b []byte) error {
	var assetMapList []map[string]*json.RawMessage

	// Partial unmarshal to map of json RawMessage
	err := json.Unmarshal(b, &assetMapList)
	if err != nil {
		return err
	}

	var assetSetList []*AssetSetResponse

	for _, rawm := range assetMapList {

		var asresp AssetSetResponse
		err = asresp.RawMessageToAssetSetResponse(rawm)
		if err != nil {
			return err
		}

		assetSetList = append(assetSetList, &asresp)

	}

	asrr.Assets = assetSetList

	return nil
}

// Extra decoding util functions, for clarity

// Creates an AssetProperties directly from map[string]interface{}
func toAssetProp(fproperties map[string]interface{}) AssetProperties {
	var properties AssetProperties

	if category, v := fproperties["category"].(string); v {
		properties.Category = category
	}
	if provider, v := fproperties["provider"].(string); v {
		properties.Provider = provider
	}
	if account, v := fproperties["account"].(string); v {
		properties.Account = account
	}
	if project, v := fproperties["project"].(string); v {
		properties.Project = project
	}
	if service, v := fproperties["service"].(string); v {
		properties.Service = service
	}
	if cluster, v := fproperties["cluster"].(string); v {
		properties.Cluster = cluster
	}
	if name, v := fproperties["name"].(string); v {
		properties.Name = name
	}
	if providerID, v := fproperties["providerID"].(string); v {
		properties.ProviderID = providerID
	}

	return properties

}

// Creates an Breakdown directly from map[string]interface{}
func toBreakdown(fproperties map[string]interface{}) Breakdown {
	var breakdown Breakdown

	if idle, v := fproperties["idle"].(float64); v {
		breakdown.Idle = idle
	}
	if other, v := fproperties["other"].(float64); v {
		breakdown.Other = other
	}
	if system, v := fproperties["system"].(float64); v {
		breakdown.System = system
	}
	if user, v := fproperties["user"].(float64); v {
		breakdown.User = user
	}

	return breakdown

}

// Not strictly nessesary, but cleans up the code and is a secondary check
// for correct types
func getTypedVal(itf interface{}) (interface{}, error) {
	switch itf := itf.(type) {
	case float64:
		return float64(itf), nil
	case string:
		return string(itf), nil
	default:
		unktype := reflect.ValueOf(itf)
		return nil, fmt.Errorf("Type %v is an invalid type", unktype)
	}
}
