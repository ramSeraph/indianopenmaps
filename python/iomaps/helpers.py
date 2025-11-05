import logging

import py7zr
import fiona

def get_supported_output_drivers():
    driver_list = []
    supported_drivers = fiona.supported_drivers
    for k, v in supported_drivers.items():
        # Check if the driver supports writing ('w') or appending ('a')
        if 'w' in v or 'a' in v:
            driver_list.append(k)
    return driver_list


def fix_if_required(p):
    if p.is_valid:
        return p
    p = p.buffer(0)
    if not p.is_valid:
        raise Exception('could not fix polygon')
    return p


def readable_size(size_bytes):
    if size_bytes == 0:
        return "0.00 KB"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(min(len(size_name) - 1, (size_bytes.bit_length() - 1) // 10))
    p = 1 << (i * 10)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"


class StreamingPy7zIO(py7zr.io.Py7zIO):
    def __init__(self, filename, filter, writer, pbar=None):
        self.filename = filename
        self.filter = filter
        self.writer = writer
        self._buffer = b""
        self.length = 0
        self.pbar = pbar

    def extract_lines(self):
        lines = []
        while True:
            newline_index = self._buffer.find(b'\n')
            if newline_index == -1:
                break

            line = self._buffer[:newline_index + 1]
            self._buffer = self._buffer[newline_index + 1:]

            lines.append(line.decode('utf-8').rstrip('\n'))

        return lines

    def process_line(self, line):
        output_size = None
        feature = self.filter.process(line)
        if feature is not None:
            self.writer.write(feature)
            output_size = self.writer.size()

        if self.pbar:
            if output_size is None:
                current_postfix = self.pbar.postfix
                output_size_str = current_postfix.get('output_size', '0.00 KB')
            else:
                output_size_str = readable_size(output_size)
            self.pbar.set_postfix(processed=self.filter.count,
                                  passed=self.filter.passed,
                                  output_size=output_size_str)

    def write(self, data):
        self.length += len(data)

        self._buffer += data

        lines = self.extract_lines()
        for line in lines:
            self.process_line(line)

        if self.pbar:
            self.pbar.update(len(data))

    def read(self, size=None):
        return b''

    def seek(self, offset, whence=0) -> int:
        return offset

    def flush(self):
        pass

    def size(self):
        return self.length

    def flush_last_line(self):
        if self._buffer:
            line = self._buffer
            self._buffer = b""
            self.process_line(line)

def get_base_name(input_path):
    if '.7z.001' in input_path.name:
        base_name = input_path.name.rsplit('.7z.001', 1)[0]
    else:
        base_name = input_path.name.rsplit('.7z', 1)[0]
    return base_name

def get_geojsonl_file_info(archive):
    geojsonl_files_infos = [f for f in archive.files if f.filename.endswith('.geojsonl')]

    if not geojsonl_files_infos:
        logging.error("No .geojsonl file found in the archive.")
        return None

    if len(geojsonl_files_infos) > 1:
        logging.warning(f"Multiple .geojsonl files found, using the first one: {geojsonl_files_infos[0].filename}")

    return geojsonl_files_infos[0]

