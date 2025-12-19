package util

import "fmt"

func RemoveDuplicate(list []string) []string {
	uniqueMap := make(map[string]struct{})
	result := make([]string, 0)
	for _, item := range list {
		if _, exists := uniqueMap[item]; !exists {
			uniqueMap[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}

func FormatVector(vector []float64) string {
	if len(vector) == 0 {
		return "[]"
	}

	vectorString := "["
	for i, val := range vector {
		if i > 0 {
			vectorString += ","
		}
		vectorString += fmt.Sprintf("%f", val)
	}
	vectorString += "]"

	return vectorString
}
