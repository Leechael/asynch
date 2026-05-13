from .intcolumn import Int64Column


class IntervalColumn(Int64Column):
    pass


class IntervalDayColumn(IntervalColumn):
    ch_type = "IntervalDay"


class IntervalNanosecondColumn(IntervalColumn):
    ch_type = "IntervalNanosecond"


class IntervalMicrosecondColumn(IntervalColumn):
    ch_type = "IntervalMicrosecond"


class IntervalMillisecondColumn(IntervalColumn):
    ch_type = "IntervalMillisecond"


class IntervalWeekColumn(IntervalColumn):
    ch_type = "IntervalWeek"


class IntervalMonthColumn(IntervalColumn):
    ch_type = "IntervalMonth"


class IntervalQuarterColumn(IntervalColumn):
    ch_type = "IntervalQuarter"


class IntervalYearColumn(IntervalColumn):
    ch_type = "IntervalYear"


class IntervalHourColumn(IntervalColumn):
    ch_type = "IntervalHour"


class IntervalMinuteColumn(IntervalColumn):
    ch_type = "IntervalMinute"


class IntervalSecondColumn(IntervalColumn):
    ch_type = "IntervalSecond"
