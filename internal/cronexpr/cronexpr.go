package cronexpr

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Schedule struct {
	minute field
	hour   field
	dom    field
	month  field
	dow    field
}

type field struct {
	values map[int]struct{}
	all    bool
}

func Parse(expr string) (Schedule, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return Schedule{}, errors.New("cron schedule must have 5 fields")
	}

	minute, err := parseField(parts[0], 0, 59, identity)
	if err != nil {
		return Schedule{}, fmt.Errorf("minute field: %w", err)
	}
	hour, err := parseField(parts[1], 0, 23, identity)
	if err != nil {
		return Schedule{}, fmt.Errorf("hour field: %w", err)
	}
	dom, err := parseField(parts[2], 1, 31, identity)
	if err != nil {
		return Schedule{}, fmt.Errorf("day-of-month field: %w", err)
	}
	month, err := parseField(parts[3], 1, 12, identity)
	if err != nil {
		return Schedule{}, fmt.Errorf("month field: %w", err)
	}
	dow, err := parseField(parts[4], 0, 7, normalizeDOW)
	if err != nil {
		return Schedule{}, fmt.Errorf("day-of-week field: %w", err)
	}

	return Schedule{minute: minute, hour: hour, dom: dom, month: month, dow: dow}, nil
}

func Next(expr string, after time.Time) (time.Time, error) {
	schedule, err := Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	return schedule.Next(after)
}

func (s Schedule) Next(after time.Time) (time.Time, error) {
	start := after.UTC().Truncate(time.Minute).Add(time.Minute)
	deadline := start.AddDate(5, 0, 0)
	for candidate := start; !candidate.After(deadline); candidate = candidate.Add(time.Minute) {
		if s.matches(candidate) {
			return candidate, nil
		}
	}
	return time.Time{}, errors.New("cron schedule has no matching time within 5 years")
}

func (s Schedule) matches(t time.Time) bool {
	if !s.minute.matches(t.Minute()) || !s.hour.matches(t.Hour()) || !s.month.matches(int(t.Month())) {
		return false
	}

	domMatches := s.dom.matches(t.Day())
	dowMatches := s.dow.matches(int(t.Weekday()))
	switch {
	case s.dom.all && s.dow.all:
		return true
	case s.dom.all:
		return dowMatches
	case s.dow.all:
		return domMatches
	default:
		return domMatches || dowMatches
	}
}

func parseField(raw string, minValue, maxValue int, normalize func(int) (int, error)) (field, error) {
	if raw == "" {
		return field{}, errors.New("field is empty")
	}

	result := field{
		values: map[int]struct{}{},
		all:    raw == "*",
	}

	for _, part := range strings.Split(raw, ",") {
		if part == "" {
			return field{}, errors.New("empty list item")
		}
		rangePart := part
		step := 1
		if strings.Contains(part, "/") {
			pieces := strings.Split(part, "/")
			if len(pieces) != 2 || pieces[0] == "" || pieces[1] == "" {
				return field{}, fmt.Errorf("invalid step syntax %q", part)
			}
			parsedStep, err := strconv.Atoi(pieces[1])
			if err != nil || parsedStep <= 0 {
				return field{}, fmt.Errorf("invalid step %q", pieces[1])
			}
			rangePart = pieces[0]
			step = parsedStep
		}

		start, end, err := parseRange(rangePart, minValue, maxValue)
		if err != nil {
			return field{}, err
		}
		for value := start; value <= end; value += step {
			normalized, err := normalize(value)
			if err != nil {
				return field{}, err
			}
			result.values[normalized] = struct{}{}
		}
	}

	if len(result.values) == 0 {
		return field{}, errors.New("field has no values")
	}
	return result, nil
}

func parseRange(raw string, minValue, maxValue int) (int, int, error) {
	if raw == "*" {
		return minValue, maxValue, nil
	}
	if strings.Contains(raw, "-") {
		pieces := strings.Split(raw, "-")
		if len(pieces) != 2 || pieces[0] == "" || pieces[1] == "" {
			return 0, 0, fmt.Errorf("invalid range %q", raw)
		}
		start, err := parseBound(pieces[0], minValue, maxValue)
		if err != nil {
			return 0, 0, err
		}
		end, err := parseBound(pieces[1], minValue, maxValue)
		if err != nil {
			return 0, 0, err
		}
		if end < start {
			return 0, 0, fmt.Errorf("range %q ends before it starts", raw)
		}
		return start, end, nil
	}
	value, err := parseBound(raw, minValue, maxValue)
	if err != nil {
		return 0, 0, err
	}
	return value, value, nil
}

func parseBound(raw string, minValue, maxValue int) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", raw)
	}
	if value < minValue || value > maxValue {
		return 0, fmt.Errorf("value %d outside range %d-%d", value, minValue, maxValue)
	}
	return value, nil
}

func (f field) matches(value int) bool {
	_, ok := f.values[value]
	return ok
}

func identity(value int) (int, error) {
	return value, nil
}

func normalizeDOW(value int) (int, error) {
	if value == 7 {
		return 0, nil
	}
	return value, nil
}
