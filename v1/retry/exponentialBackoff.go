package retry

// [15,15,30,180,1800,3600,5400,7200,14400]
// [14400,7200,5400,3600,1800,180,30,15,15]
// var internal = []int{14400, 7200, 5400, 3600, 1800, 180, 30, 15, 15}

type TransExponentialBackoff []int

var NotificationInternals = TransExponentialBackoff([]int{15, 10, 5})

//func (t *TransExponentialBackoff) TransNotificationBackoff(retryCount int) (int, error) {
//	if retryCount > len(*t) {
//		return 0, fmt.Errorf("retryCount invalid")
//	}
//	s := []int(*t)
//
//	return s[retryCount], nil
//}

func (t *TransExponentialBackoff) RetryCount() int {
	return len(*t)
}

func TransNotificationBackoff(retryCount int) (int, error) {
	//if retryCount > len(NotificationInternals) {
	//	return 0, fmt.Errorf("retryCount invalid")
	//}
	return NotificationInternals[retryCount], nil
}
