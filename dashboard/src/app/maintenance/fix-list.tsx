'use client'

import { useState } from 'react';
import { updateTherapistField } from './actions';
import { validateZip, validateCanton } from '@/lib/validation'; // Client-side validation helper

export function FixList({ rows }: { rows: any[] }) {
    const [editingId, setEditingId] = useState<string | null>(null);
    const [editValue, setEditValue] = useState('');
    const [loading, setLoading] = useState(false);

    const startEdit = (row: any) => {
        setEditingId(row.id);
        setEditValue(row.value || '');
    };

    const handleSave = async (row: any) => {
        setLoading(true);
        await updateTherapistField(row.id, row.field, editValue);
        setLoading(false);
        setEditingId(null);
    };

    return (
        <table style={{ width: '100%', borderCollapse: 'collapse', color: '#ccc' }}>
            <thead>
                <tr style={{ borderBottom: '1px solid var(--card-border)', textAlign: 'left' }}>
                    <th style={{ padding: '1rem' }}>Name</th>
                    <th style={{ padding: '1rem' }}>Current Value</th>
                    <th style={{ padding: '1rem' }}>New Value</th>
                    <th style={{ padding: '1rem' }}>Action</th>
                </tr>
            </thead>
            <tbody>
                {rows.map(row => (
                    <tr key={row.id} style={{ borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                        <td style={{ padding: '1rem' }}>{row.firstName} {row.lastName}</td>
                        <td style={{ padding: '1rem', color: 'var(--error)' }}>
                            {row.value === null ? <em>NULL</em> : row.value}
                        </td>
                        <td style={{ padding: '1rem' }}>
                            {editingId === row.id ? (
                                <input
                                    autoFocus
                                    value={editValue}
                                    onChange={e => setEditValue(e.target.value)}
                                    style={{
                                        padding: '0.5rem',
                                        borderRadius: '4px',
                                        border: '1px solid var(--primary)',
                                        background: 'black',
                                        color: 'white'
                                    }}
                                />
                            ) : (
                                <span style={{ color: '#666' }}>Click Edit</span>
                            )}
                        </td>
                        <td style={{ padding: '1rem' }}>
                            {editingId === row.id ? (
                                <div style={{ display: 'flex', gap: '0.5rem' }}>
                                    <button
                                        onClick={() => handleSave(row)}
                                        disabled={loading}
                                        className="btn btn-primary"
                                        style={{ padding: '0.25rem 0.75rem', fontSize: '0.875rem' }}
                                    >
                                        Save
                                    </button>
                                    <button
                                        onClick={() => setEditingId(null)}
                                        className="btn"
                                        style={{ padding: '0.25rem 0.75rem', fontSize: '0.875rem', background: '#333' }}
                                    >
                                        Cancel
                                    </button>
                                </div>
                            ) : (
                                <button
                                    onClick={() => startEdit(row)}
                                    className="btn"
                                    style={{ padding: '0.25rem 0.75rem', fontSize: '0.875rem', background: 'var(--card-bg)', border: '1px solid var(--card-border)' }}
                                >
                                    Edit
                                </button>
                            )}
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}
