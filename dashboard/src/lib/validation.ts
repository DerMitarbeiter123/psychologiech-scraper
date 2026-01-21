export const CANTONS = [
    'AG', 'AI', 'AR', 'BE', 'BL', 'BS', 'FR', 'GE', 'GL', 'GR', 'JU', 'LU', 'NE', 'NW',
    'OW', 'SG', 'SH', 'SO', 'SZ', 'TG', 'TI', 'UR', 'VD', 'VS', 'ZG', 'ZH'
] as const;

export type ValidationResult = {
    isValid: boolean;
    error?: string;
    normalized?: string;
};

export function validateZip(zip: string | null | undefined): ValidationResult {
    if (!zip) return { isValid: false, error: 'Empty ZIP' };
    const cleaned = zip.trim();
    const isSwiss = /^\d{4}$/.test(cleaned);
    // Optional: Check range (1000-9999)
    if (isSwiss) return { isValid: true, normalized: cleaned };
    return { isValid: false, error: 'Invalid Format (must be 4 digits)' };
}

export function validateCanton(canton: string | null | undefined): ValidationResult {
    if (!canton) return { isValid: false, error: 'Empty Canton' };
    const upper = canton.toUpperCase().trim();
    if (CANTONS.includes(upper as any)) return { isValid: true, normalized: upper };
    return { isValid: false, error: 'Invalid Canton Code' };
}

export function validateEmail(email: string | null | undefined): ValidationResult {
    if (!email) return { isValid: true, normalized: undefined }; // Email not mandatory? Assume check existing
    const cleaned = email.trim();
    // Basic regex
    const regex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (regex.test(cleaned)) return { isValid: true, normalized: cleaned };
    return { isValid: false, error: 'Invalid Email Format' };
}

export function validatePhone(phone: string | null | undefined): ValidationResult {
    if (!phone) return { isValid: true, normalized: undefined }; // Optional usually
    // Remove spaces, dashes, parens
    const cleaned = phone.replace(/[\s\-\(\)\.]/g, '');
    // Swiss format check: +41 or 0xx
    // Just generic length check for now or basic E.164
    if (cleaned.length < 9) return { isValid: false, error: 'Too Short' };
    return { isValid: true, normalized: cleaned };
}
