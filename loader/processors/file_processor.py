class FileProcessor:
    def __init__(self, reader, mapper):
        self.reader = reader
        self.mapper = mapper
    
    def process(self, file_path):
        raw_data = self.reader.read(file_path)
        return self.mapper.transform(raw_data)