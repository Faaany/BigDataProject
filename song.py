class Song:
  def __init__(self, line):
    data = line.split(",")
    Position = data[0]
    Name = data[1]
    Artist = data[2]
    Streams = data[3]
    Date = data[5]
    Region = data[6]
  Position = ""
  Name = ""
  Artist = ""
  Streams = ""
  Date = ""
  Region = ""
