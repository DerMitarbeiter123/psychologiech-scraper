import { prisma } from '@/lib/prisma';

export default async function DataPage() {
    const therapists = await prisma.therapist.findMany({
        take: 50,
        orderBy: { createdAt: 'desc' }
    });

    return (
        <div className="container">
            <h1 className="landing-header">Data Browser</h1>
            <div className="glass-panel" style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: '800px' }}>
                    <thead>
                        <tr style={{ background: 'rgba(255,255,255,0.02)' }}>
                            <th style={{ padding: '1rem', textAlign: 'left' }}>Details</th>
                            <th style={{ padding: '1rem', textAlign: 'left' }}>Location</th>
                            <th style={{ padding: '1rem', textAlign: 'left' }}>Contact</th>
                            <th style={{ padding: '1rem', textAlign: 'left' }}>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {therapists.map(t => (
                            <tr key={t.id} style={{ borderBottom: '1px solid var(--glass-border)' }}>
                                <td style={{ padding: '1rem' }}>
                                    <div style={{ fontWeight: 'bold' }}>{t.firstName} {t.lastName}</div>
                                    <div style={{ fontSize: '0.85rem', color: '#888' }}>{t.title}</div>
                                </td>
                                <td style={{ padding: '1rem' }}>
                                    <div>{t.street}</div>
                                    <div style={{ color: 'var(--accent)' }}>{t.zip} {t.city} ({t.canton})</div>
                                </td>
                                <td style={{ padding: '1rem' }}>
                                    {t.email && <div>{t.email}</div>}
                                    {t.phone && <div style={{ color: '#888' }}>{t.phone}</div>}
                                </td>
                                <td style={{ padding: '1rem' }}>
                                    {t.contactVerified ?
                                        <span style={{ color: 'var(--success)' }}>Verified</span> :
                                        <span style={{ color: '#666' }}>Unverified</span>
                                    }
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
