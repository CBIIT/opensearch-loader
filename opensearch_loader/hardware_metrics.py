"""Best-effort, read-only container diagnostics; no third-party dependencies."""

import json
import os
import platform
import threading
import time
from contextlib import contextmanager
from pathlib import Path


def _read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def _number(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _controller_root(controller, root):
    # Support both namespace-root mounts and /proc/self/cgroup relative paths.
    candidates = [root / controller]
    if controller == 'cpu':
        candidates.append(root / 'cpu,cpuacct')
    for line in (_read('/proc/self/cgroup') or '').splitlines():
        _, controllers, relative = line.split(':', 2)
        if controller in controllers.split(','):
            candidates = [p / relative.lstrip('/') for p in candidates] + candidates
    return next((p for p in candidates if p.is_dir()), root / controller)


def container_metrics(root=Path('/sys/fs/cgroup')):
    """None means unavailable or unlimited, never the host's memory allocation."""
    result = {}
    if (root / 'cgroup.controllers').exists():
        base = root
        for line in (_read('/proc/self/cgroup') or '').splitlines():
            if line.startswith('0::'):
                candidate = root / line[3:].lstrip('/')
                if (candidate / 'cpu.max').exists():
                    base = candidate
        quota = (_read(base / 'cpu.max') or '').split()
        result['cgroup_version'] = 2
        result['cpu_quota_cores'] = (
            int(quota[0]) / int(quota[1])
            if len(quota) == 2 and quota[0] != 'max' else None
        )
        memory_limit = _number(_read(base / 'memory.max'))
        result['container_memory_current_bytes'] = _number(_read(base / 'memory.current'))
        stats = _read(base / 'cpu.stat')
        result['memory_events'] = _read(base / 'memory.events')
    else:
        cpu = _controller_root('cpu', root)
        memory = _controller_root('memory', root)
        quota = _number(_read(cpu / 'cpu.cfs_quota_us'))
        period = _number(_read(cpu / 'cpu.cfs_period_us'))
        result['cgroup_version'] = 1 if cpu.exists() or memory.exists() else None
        result['cpu_quota_cores'] = quota / period if quota and quota > 0 and period else None
        memory_limit = _number(_read(memory / 'memory.limit_in_bytes'))
        result['container_memory_current_bytes'] = _number(_read(memory / 'memory.usage_in_bytes'))
        stats = _read(cpu / 'cpu.stat')
    # cgroup v1 uses an enormous sentinel for unlimited memory.
    result['container_memory_limit_bytes'] = (
        memory_limit if memory_limit is not None and memory_limit < 2**60 else None
    )
    result['cpu_stat_cumulative'] = {
        key: int(value) for key, value in (line.split() for line in (stats or '').splitlines())
    }
    # v1 throttled_time is ns; v2 throttled_usec is microseconds.
    result['cpu_stat_time_unit'] = 'microseconds' if result['cgroup_version'] == 2 else 'nanoseconds'
    for line in (_read('/proc/self/status') or '').splitlines():
        if line.startswith('VmRSS:'):
            result['process_rss_bytes'] = int(line.split()[1]) * 1024
    return result


@contextmanager
def hardware_monitor(logger, interval=60):
    """Capture the Prefect run logger on the flow thread, then reuse it safely."""
    stop = threading.Event()
    previous_wall, previous_cpu = time.monotonic(), time.process_time()

    def emit(event):
        nonlocal previous_wall, previous_cpu
        try:
            now, cpu = time.monotonic(), time.process_time()
            values = container_metrics()
            values.update(event=event, scope='loader_container')
            values['process_cpu_percent_one_core'] = round(
                100 * (cpu - previous_cpu) / max(now - previous_wall, 0.001), 2
            ) if event != 'start' else None
            previous_wall, previous_cpu = now, cpu
            if event == 'start':
                values.update(
                    architecture=platform.machine(), python=platform.python_version(),
                    os=platform.system(), host_visible_logical_cpus=os.cpu_count(),
                    affinity_cpus=len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
                    sample_interval_seconds=interval,
                )
            logger.info('HARDWARE_METRICS %s', json.dumps(values, sort_keys=True))
        except Exception:
            # Diagnostics must never abort an index load.
            logger.warning('HARDWARE_METRICS collection unavailable', exc_info=True)

    def sample():
        while not stop.wait(interval):
            emit('sample')

    emit('start')
    thread = threading.Thread(target=sample, name='hardware-metrics', daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()
        emit('end')
