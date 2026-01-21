import { prisma } from '@/lib/prisma';
import { FixList } from './fix-list';

type Props = {
    searchParams: Promise<{ check?: string }>;
}; // In Next.js 15 searchParams is a promise

export default async function MaintenancePage(props: Props) {
    const searchParams = await props.searchParams;
    const check = searchParams.check || 'zip';

    let invalidRows = [];
    let description = '';

    // Logic to fetch invalid data
    if (check === 'zip') {
        description = 'Therapists with ZIP codes that are not exactly 4 digits.';
        invalidRows = await prisma.$queryRaw<any[]>`
      SELECT id, "firstName", "lastName", zip as "value", 'zip' as "field"
      FROM "Therapist"
      WHERE length(zip) != 4 OR zip IS NULL
      LIMIT 100
    `;
    } else if (check === 'canton') {
        description = 'Therapists with Canton codes that are not 2 uppercase letters.';
        // Approximate SQL check
        invalidRows = await prisma.$queryRaw<any[]>`
      SELECT id, "firstName", "lastName", canton as "value", 'canton' as "field"
      FROM "Therapist"
      WHERE length(canton) != 2 OR canton IS NULL
      LIMIT 100
    `;
    }

    // Convert BigInt if strictly needed (Prisma raw query returns BigInt for count? No, select fields generally fine unless count)
    // But raw query might return complex objects.

    return (
        <div className="container">
            <h1 className="landing-header">Data Maintenance</h1>
            <p style={{ color: '#a1a1aa', marginBottom: '2rem' }}>{description}</p>

            <div style={{ display: 'flex', gap: '1rem', marginBottom: '2rem' }}>
                <a href="?check=zip" className={`btn ${check === 'zip' ? 'btn-primary' : 'glass-panel'}`}>Fix Zips</a>
                <a href="?check=canton" className={`btn ${check === 'canton' ? 'btn-primary' : 'glass-panel'}`}>Fix Cantons</a>
            </div>

            <div className="glass-panel" style={{ padding: '1rem' }}>
                {invalidRows.length === 0 ? (
                    <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--success)' }}>
                        All clear! No issues found for this check.
                    </div>
                ) : (
                    <FixList rows={invalidRows} />
                )}
            </div>
        </div>
    );
}
