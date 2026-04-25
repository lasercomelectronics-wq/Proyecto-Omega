from __future__ import annotations

import os
from pathlib import Path


class SingleInstanceError(RuntimeError):
    pass


class SingleInstanceLock:
    def __init__(self, lock_path: Path) -> None:
        self._lock_path = lock_path
        self._fd: int | None = None
        self.last_stale_pid: int | None = None

    @staticmethod
    def _pid_running(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def acquire(self) -> None:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                self._fd = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self._fd, str(os.getpid()).encode("utf-8"))
                return
            except FileExistsError:
                try:
                    raw = self._lock_path.read_text(encoding="utf-8").strip()
                    pid = int(raw) if raw else 0
                except (OSError, ValueError):
                    pid = 0
                if pid and self._pid_running(pid):
                    raise SingleInstanceError(f"Bot ya está corriendo. PID: {pid}")
                self.last_stale_pid = pid or None
                try:
                    self._lock_path.unlink()
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    raise SingleInstanceError(f"No se pudo limpiar lock stale: {exc}") from exc

    def release(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            finally:
                self._fd = None
        try:
            if self._lock_path.exists():
                self._lock_path.unlink()
        except OSError:
            return
