import telegram
import logging
import socket
from datetime import datetime
from time import time
from numpipe import config
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

DEFAULT_DELAY = config.get_config()['notifications']['default_delay']

def get_bot_token():
    return config.get_config()['notifications']['telegram']['token']

def get_chat_id():
    return config.get_config()['notifications']['telegram']['chat_id']

def notifications_active():
    return get_bot_token() and get_chat_id()

def generate_time_str(time):
    hours = int(time // 60**2)
    minutes = int((time - hours*60**2) // 60)
    seconds = int((time - hours*60**2 - minutes*60) // 1)

    ret = ''
    if hours:
        unit = 'hr' if hours == 1 else 'hrs'
        ret += f'{hours} {unit} '
    if minutes:
        unit = 'min' if minutes == 1 else 'mins'
        ret += f'{minutes} {unit} '

    if not (hours and minutes):
        unit = 'sec' if seconds == 1 else 'secs'
        ret += f'{seconds} {unit}'

    return ret.strip()

def check_idle_matplotlib(delay=DEFAULT_DELAY, check_every=.5):
    """
    Check if the user is idle based on matplotlib plot interactions (mouse move, key press, window interactions)

    Arguments:
        delay         time (in seconds) to check before declaring idle
        check_every   time (in seconds) between interaction checks
    """
    import matplotlib.pyplot as plt
    from time import sleep

    mouse_moved = False
    key_pressed = False
    def on_mouse_movement(event):
        nonlocal mouse_moved
        mouse_moved = True

    def on_key_press(event):
        nonlocal key_pressed
        key_pressed = True

    nfigures_before = len(plt.get_fignums())
    fig = plt.figure(nfigures_before)
    cid = fig.canvas.mpl_connect('motion_notify_event', on_mouse_movement)
    cid = fig.canvas.mpl_connect('key_press_event', on_key_press)

    sleep(1)
    x0 = fig.canvas.manager.window.x()
    y0 = fig.canvas.manager.window.y()
    w0 = fig.canvas.manager.window.size()

    t_start = time()
    while time() - t_start < delay:
        sleep(check_every)
        if len(plt.get_fignums()) != nfigures_before:
            return False
        if mouse_moved:
            return False
        if key_pressed:
            return False
        if not fig.canvas.manager.window.isActiveWindow():
            return False

        x = fig.canvas.manager.window.x()
        y = fig.canvas.manager.window.y()
        w = fig.canvas.manager.window.size()

        if x != x0 or y != y0:
            return False

        if w != w0:
            return False

    return True

def send_finish_message(filename, njobs, time, num_exceptions):
    bot = telegram.Bot(token=get_bot_token())
    host = socket.gethostname()
    time_str = generate_time_str(time)
    tab = '    '

    if num_exceptions:
        status = f'{num_exceptions}/{njobs} failures'
    else:
        status = 'success'

    date = datetime.now().strftime("%H:%M %d-%m-%Y")

    text = f'''`Simulation finished:
{tab}filename___{filename}.py
{tab}status_____{status}
{tab}host_______{host}
{tab}njobs______{njobs}
{tab}runtime____{time_str}
{tab}date_______{date}`
'''
    bot.send_message(chat_id=get_chat_id(), text=text, parse_mode=telegram.ParseMode.MARKDOWN)

def send_images(filename):
    from io import BytesIO
    import matplotlib.pyplot as plt

    media = []
    for i in plt.get_fignums():
        fig = plt.figure(i)
        caption = f'{filename}-fig{i}'

        bio = BytesIO()
        bio.name = f'fig{i}.png'
        fig.savefig(bio, format='png')
        bio.seek(0)
        media.append(telegram.InputMediaPhoto(bio, caption=caption))

    bot = telegram.Bot(token=get_bot_token())
    # bot.send_photo(get_chat_id(), photo=bio)
    bot.send_media_group(get_chat_id(), media=media)

def send_video():
    pass

def send_notifications(notifications, delay=DEFAULT_DELAY, check_idle=True, idle=False):
    if not notifications_active():
        return

    if check_idle:
        idle = check_idle_matplotlib(delay=delay)

    if idle:
        for notification in notifications:
            notification()
