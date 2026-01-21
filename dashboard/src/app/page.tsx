import { prisma } from '@/lib/prisma';
import Link from 'next/link';

// Simple approx validations for stats
async function getStats() {
  const total = await prisma.therapist.count();

  // Approximate SQL checks
  const invalidZip = await prisma.therapist.count({
    where: {
      OR: [
        { zip: null },
        { zip: { equals: '' } } // Prisma doesn't do length check easily without raw, but let's assume raw or just simple approximation for now.
        // Actually, let's just stick to null/empty or check logic in a specific tool for "Deep Scan".
        // For dashboard, we might want to run a raw query for speed if we want validation counts.
      ]
    }
  });

  // Let's us Raw query for "Rows with Invalid Zip" (not 4 chars)
  const invalidZipExact = await prisma.$queryRaw<{ count: bigint }[]>`
    SELECT COUNT(*)::int as count FROM "Therapist" WHERE length(zip) != 4 OR zip IS NULL
  `;

  // Invalid Canton (not 2 chars uppercase)
  const invalidCanton = await prisma.$queryRaw<{ count: bigint }[]>`
    SELECT COUNT(*)::int as count FROM "Therapist" WHERE length(canton) != 2 OR canton IS NULL
  `;

  return {
    total,
    invalidZip: Number(invalidZipExact[0].count),
    invalidCanton: Number(invalidCanton[0].count),
  };
}

export default async function Home() {
  const stats = await getStats();

  return (
    <div className="container">
      <h1 className="landing-header">Dashboard</h1>

      <div className="grid-cols-3" style={{ marginBottom: '2rem' }}>
        <div className="glass-panel" style={{ padding: '1.5rem' }}>
          <h3 style={{ color: '#a1a1aa' }}>Total Therapists</h3>
          <p style={{ fontSize: '2.5rem', fontWeight: 'bold' }}>{stats.total.toLocaleString()}</p>
        </div>

        <div className="glass-panel" style={{ padding: '1.5rem', borderColor: stats.invalidZip > 0 ? 'var(--warning)' : undefined }}>
          <h3 style={{ color: '#a1a1aa' }}>Invalid Zips</h3>
          <p style={{ fontSize: '2.5rem', fontWeight: 'bold', color: stats.invalidZip > 0 ? 'var(--warning)' : 'inherit' }}>
            {stats.invalidZip}
          </p>
          <Link href="/maintenance?check=zip" style={{ fontSize: '0.9rem', color: 'var(--accent)', marginTop: '0.5rem', display: 'block' }}>
            Fix Issues &rarr;
          </Link>
        </div>

        <div className="glass-panel" style={{ padding: '1.5rem', borderColor: stats.invalidCanton > 0 ? 'var(--warning)' : undefined }}>
          <h3 style={{ color: '#a1a1aa' }}>Invalid Cantons</h3>
          <p style={{ fontSize: '2.5rem', fontWeight: 'bold', color: stats.invalidCanton > 0 ? 'var(--warning)' : 'inherit' }}>
            {stats.invalidCanton}
          </p>
          <Link href="/maintenance?check=canton" style={{ fontSize: '0.9rem', color: 'var(--accent)', marginTop: '0.5rem', display: 'block' }}>
            Fix Issues &rarr;
          </Link>
        </div>
      </div>
    </div>
  );
}
