import py7zr


class StreamingPy7zIO(py7zr.io.Py7zIO):
    def __init__(self, filename, filter, writer, updater=None):
        self.filename = filename
        self.filter = filter
        self.writer = writer
        self._buffer = b""
        self.length = 0
        self.updater = updater

    def extract_lines(self):
        lines = []
        while True:
            newline_index = self._buffer.find(b"\n")
            if newline_index == -1:
                break

            line = self._buffer[: newline_index + 1]
            self._buffer = self._buffer[newline_index + 1 :]

            lines.append(line.decode("utf-8").rstrip("\n"))

        return lines

    def process_line(self, line):
        feature = self.filter.process(line)
        if feature is not None:
            self.writer.write(feature)

        output_size = self.writer.size()

        if self.updater:
            self.updater.update_other_info(
                processed=self.filter.count,
                passed=self.filter.passed,
                output_size=output_size,
            )

    def write(self, data):
        self.length += len(data)

        self._buffer += data

        lines = self.extract_lines()
        for line in lines:
            self.process_line(line)

        if self.updater:
            self.updater.update_size(len(data))

    def read(self, size=None):
        return b""

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


class StreamingWriterFactory(py7zr.io.WriterFactory):
    def __init__(self, filter, writer, status_updater):
        self.streaming_io = None
        self.filter = filter
        self.writer = writer
        self.status_updater = status_updater

    def create(self, filename):
        self.streaming_io = StreamingPy7zIO(filename, self.filter, self.writer, self.status_updater)
        return self.streaming_io
