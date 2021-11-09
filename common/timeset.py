from datetime import datetime, timedelta
from zoneinfo import ZoneInfo # requires python3.9


class TimeSet:
    """
    Manage time related utilities
    """

    utc = "UTC"

    def str2date(self, string, dt_format, tz, as_tz=None):
        """
        conevrts string based date to datetime base on defined datetime formate, and timezone
        """
        d = datetime.strptime(string, dt_format)
        tzinfo = ZoneInfo(tz)
        self.tz_date = datetime(
            year=d.year,
            month=d.month,
            day=d.day,
            hour=d.hour,
            minute=d.minute,
            second=d.second,
            tzinfo=tzinfo,
        )

        if as_tz:
            as_tz_z = ZoneInfo(as_tz)
            atz_date = self.tz_date.astimezone(as_tz_z)
            return atz_date

        return self.tz_date

    def int2str_time(self, i_time: int, formate: str = None):
        d = datetime.fromtimestamp(i_time / 1000)
        # dd/MM/yyyy HH:mm:ss
        return d.strftime(formate or "%d/%m/%Y %H:%M:%S.%f")
