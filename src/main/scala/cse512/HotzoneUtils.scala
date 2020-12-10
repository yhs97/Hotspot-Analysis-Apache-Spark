package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    var rectangleCoord = queryRectangle.split(',');

    //Rectangle coordinates
    var x1 = rectangleCoord(0).toDouble; var y1 = rectangleCoord(1).toDouble;
    var x2 = rectangleCoord(2).toDouble; var y2 = rectangleCoord(3).toDouble;

    var pointCoord = pointString.split(',');

    //Point coordinates
    var x = pointCoord(0).toDouble; var y = pointCoord(1).toDouble;

    //Check if point is inside rectangle
    if((x>=x1 && x<=x2) || (x>=x2 && x<=x1)){
      if((y>=y1 && y<=y2) || (y>=y2 && y<=y1)){
        return true;
      }
    }


    return false; // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART IF YOU WANT TO ADD ADDITIONAL METHODS

}
