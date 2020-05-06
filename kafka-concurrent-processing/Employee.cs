using System;

namespace KafkaConcurrentProcessing
{
	public class Employee
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public string Department { get; set; }
		public string Email { get; set; }
		public byte[] RowVersion { get; set; }
  }
}