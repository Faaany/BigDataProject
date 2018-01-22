class Song:
  def __init__(self, line):
    try:
      data = line.split(",")
      self.Position = data[0]
      self.Name = data[1]
      self.Artist = data[2]
      self.Streams = data[3]
      self.Date = data[5]
      self.Region = data[6]
    except:
      pass
  Position = ""
  Name = ""
  Artist = ""
  Streams = ""
  Date = ""
  Region = ""
