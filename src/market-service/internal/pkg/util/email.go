package util

import "strings"

// hideEmail 隐藏邮箱地址，只显示前4个字符和后4个字符，例如 ramsey@123.com -> rams*******.com
func HideEmail(email string) string {
	if email == "" {
		return ""
	}

	emailLen := len(email)
	if emailLen <= 8 {
		return email // 如果邮箱总长度<=8，直接返回原邮箱
	}

	// 保留前4个字符和后4个字符，中间用*替换
	frontPart := email[:4]
	backPart := email[emailLen-4:]
	hiddenLen := emailLen - 8
	hiddenPart := strings.Repeat("*", hiddenLen)

	return frontPart + hiddenPart + backPart
}
