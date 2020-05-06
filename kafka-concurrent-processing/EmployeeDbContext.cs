using Microsoft.EntityFrameworkCore;

namespace KafkaConcurrentProcessing
{
	public class EmployeeDbContext : DbContext
	{
		public DbSet<Employee> Employees { get; set; }

		public EmployeeDbContext(DbContextOptions<EmployeeDbContext> options) : base(options) {}

		protected override void OnModelCreating(ModelBuilder modelBuilder)
		{
			modelBuilder.ApplyConfiguration(new EmployeeConfiguration());
			base.OnModelCreating(modelBuilder);
		}
	}
}