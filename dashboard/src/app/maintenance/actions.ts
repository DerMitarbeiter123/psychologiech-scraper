'use server'

import { prisma } from '@/lib/prisma';
import { revalidatePath } from 'next/cache';

export async function updateTherapistField(id: string, field: string, value: string) {
    try {
        await prisma.therapist.update({
            where: { id },
            data: { [field]: value }
        });
        revalidatePath('/maintenance');
        return { success: true };
    } catch (e) {
        console.error(e);
        return { success: false, error: 'Failed to update' };
    }
}
