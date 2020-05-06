using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace KafkaConcurrentProcessing
{
	public class EmployeeConfiguration : IEntityTypeConfiguration<Employee>
	{
		public void Configure(EntityTypeBuilder<Employee> builder)
		{
			builder.ToTable("Employee");

			//composite key configuration
			builder.HasKey(e => new { e.Id });
			builder.Property(e => e.Id).HasColumnName("Id");

			//column configuration
			builder
					.Property(e => e.Name);

			builder
					.Property(e => e.Department);

			builder
					.Property(e => e.Email);

			builder.Property(e => e.RowVersion)
					.IsConcurrencyToken()
					.ValueGeneratedOnAddOrUpdate();
		}
	}
}
