#%%
from datetime import datetime
import pytz

# %%

def convert_timezone(
        time_in,
        tz_identifier: str,
        new_tz_identifier: str ):
    """Convert epoch time from one time zone to another. Using pytz library, 
    which implements the Olson time zone database. tz identifiers are strings
    from the database.
    See https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    for a list of time zones.

    Parameters:
    -----------
    time : float
        Epoch time in seconds.
    tz_identifier : str
        The time zone identifier for the current time zone.
    new_tz_identifier : str
        The time zone identifier for the new time zone.
    
    Returns:
    --------
    new_time : float
        The float time in the new time zone.
    # """
    # time_obj = datetime.fromisoformat(time)
    # print(time_obj.strftime("%Y-%m-%d %H:%M:%S %Z%z"))
    # Create a datetime object from current time zone
    current_time_zone = pytz.timezone(tz_identifier)
    dt_obj = datetime.fromisoformat(time, tz=current_time_zone)
    print(dt_obj.strftime("%Y-%m-%d %H:%M:%S %Z%z"))
    print(f"time stamp: {dt_obj.timestamp()}")
    # Convert to the new time zone
    new_time_zone = pytz.timezone(new_tz_identifier)
    dt_obj_new_tz = dt_obj.astimezone(tz=new_time_zone)
    # print(dt_obj_new_tz)
    print(dt_obj_new_tz.strftime("%Y-%m-%d %H:%M:%S %Z%z"))
    # Convert the new datetime object back to epoch time
    time_new_tz = dt_obj_new_tz.timestamp()
    print(time_new_tz)
    return time_new_tz

# %% examples

# Example usage
epoch_time = 1632022461  # Sample epoch time
current_tz = "US/Central"  # The current time zone
new_tz = "US/Pacific"  # The time zone you want to convert to

# print the string
# print(f"Current epoch time in {datetime.fromtimestamp(epoch_time)}")

new_epoch_time = convert_timezone(epoch_time, current_tz, new_tz)
# print(f"New epoch time in     {datetime.fromtimestamp(new_epoch_time)}")

# %%
time_test = datetime.fromtimestamp(epoch_time, tz=pytz.timezone('US/Central'))
tz = pytz.timezone('US/Eastern')
# %%
time_str = "2021-06-18 22:34:21"
format_str = "%Y-%m-%d %H:%M:%S"

time_obj = datetime.strptime(time_str, format_str)
print(time_obj.strftime("%Y-%m-%d %H:%M:%S"))
ny_tz = pytz.timezone('America/New_York')
time_obj = ny_tz.localize(time_obj)
print(time_obj.strftime("%Y-%m-%d %H:%M:%S %Z%z"))
print(f"time stamp: {time_obj.timestamp()}")

time_obj = time_obj.astimezone(tz=pytz.timezone('US/Pacific'))
print(time_obj.strftime("%Y-%m-%d %H:%M:%S %Z%z"))
print(f"time stamp: {time_obj.timestamp()}")
# %%
