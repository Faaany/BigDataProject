import datetime

def parseDate(DateString):
  DateList = list(map(lambda x: int(x),DateString.split("-")))
  return datetime.date(DateList[0],DateList[1],DateList[2])

class Song:
  def __init__(self, line):
    try:
      data = line.split(",")
      self.Position = int(data[0])
      self.Name = data[1]
      self.Artist = data[2]
      self.Streams = int(data[3])
      self.URL = data[4]
      self.Date = parseDate(data[5])
      self.Region = data[6]
    except:
      pass
  def __str__(self):
    return "{Position},{Name},{Artist},{Streams},{URL},{Date},{Region}".format(
      Position = str(self.Position),
      Name = self.Name.encode("utf-8"),
      Artist = self.Artist.encode("utf-8"),
      Streams = str(self.Streams),
      URL = self.URL,
      Date = str(self.Date),
      Region = self.Region
    )
  Position = ""
  Name = ""
  Artist = ""
  Streams = ""
  URL = ""
  Date = datetime.date(1980,1,1)
  Region = ""
