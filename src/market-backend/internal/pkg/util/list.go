package util

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
