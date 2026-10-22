import ctypes
import ctypes.wintypes
import psutil

user32 = ctypes.windll.user32

pbi_pids = set()
for p in psutil.process_iter(['pid', 'name']):
    if 'pbidesktop' in p.info['name'].lower():
        pbi_pids.add(p.info['pid'])

print("Active PBIDesktop PIDs:", pbi_pids)

found_windows = []

def enum_cb(hwnd, lparam):
    pid = ctypes.wintypes.DWORD()
    user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
    if pid.value in pbi_pids:
        length = user32.GetWindowTextLengthW(hwnd)
        buf = ctypes.create_unicode_buffer(length + 1)
        user32.GetWindowTextW(hwnd, buf, length + 1)
        visible = user32.IsWindowVisible(hwnd)
        found_windows.append((hwnd, pid.value, visible, buf.value))
        if visible or buf.value:
            print(f"Bringing to front: HWND={hwnd}, Title='{buf.value}', Visible={visible}")
            user32.ShowWindow(hwnd, 3) # SW_MAXIMIZE = 3, SW_SHOWNORMAL = 1
            user32.SetForegroundWindow(hwnd)
    return True

WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM)
user32.EnumWindows(WNDENUMPROC(enum_cb), 0)

if not found_windows:
    print("No HWND found for PBIDesktop.")
else:
    print(f"Total {len(found_windows)} windows checked.")
