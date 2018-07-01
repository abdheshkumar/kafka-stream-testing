package kafka_producer

import java.util.Date

case class Supplier(supplierId: Int,
               supplierName: String,
               supplierStartDate: Date) {
  def getID: Int = supplierId

  def getName: String = supplierName

  def getStartDate: Date = supplierStartDate
}
