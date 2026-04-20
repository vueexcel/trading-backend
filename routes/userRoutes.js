const express = require('express');
const router = express.Router();
const requireAuth = require('../middleware/authMiddleware');
const supabase = require('../config/supabase');
const supabaseService = require('../config/supabaseService');

const AVATAR_BUCKET = 'avatars-private';
const AVATAR_URL_TTL_SEC = 60 * 10;
const AVATAR_MAX_BYTES = 5 * 1024 * 1024;
const ALLOWED_MIME = new Set(['image/jpeg', 'image/png', 'image/webp']);

function toStringOrEmpty(v) {
    return typeof v === 'string' ? v.trim() : '';
}

function inferExt(mimeType) {
    if (mimeType === 'image/png') return 'png';
    if (mimeType === 'image/webp') return 'webp';
    return 'jpg';
}

async function buildAvatarSignedUrl(avatarPath) {
    const path = toStringOrEmpty(avatarPath);
    if (!path) return '';
    try {
        const { data, error } = await supabaseService.storage
            .from(AVATAR_BUCKET)
            .createSignedUrl(path, AVATAR_URL_TTL_SEC);
        if (error) return '';
        return data?.signedUrl || '';
    } catch {
        return '';
    }
}

// Returns authenticated profile basics + display name from public.user_profiles.
router.get('/profile', requireAuth, async (req, res) => {
    try {
        const userId = req.user?.id;
        const userEmail = req.user?.email || '';

        let profile = null;
        const { data, error } = await req.supabase
            .from('user_profiles')
            .select('display_name, phone, address_line1, address_line2, city, state, postal_code, country, plan_name, plan_status, plan_renewal_at, avatar_path')
            .eq('id', userId)
            .maybeSingle();

        if (!error && data) {
            profile = data;
        }
        const displayName = toStringOrEmpty(profile?.display_name);
        const avatarPath = toStringOrEmpty(profile?.avatar_path);
        const avatarUrl = await buildAvatarSignedUrl(avatarPath);

        res.status(200).json({
            message: 'Welcome to your private profile!',
            userEmail,
            userId,
            userName: displayName || '',
            displayName: displayName || '',
            phone: toStringOrEmpty(profile?.phone),
            addressLine1: toStringOrEmpty(profile?.address_line1),
            addressLine2: toStringOrEmpty(profile?.address_line2),
            city: toStringOrEmpty(profile?.city),
            state: toStringOrEmpty(profile?.state),
            postalCode: toStringOrEmpty(profile?.postal_code),
            country: toStringOrEmpty(profile?.country),
            planName: toStringOrEmpty(profile?.plan_name),
            planStatus: toStringOrEmpty(profile?.plan_status),
            planRenewalAt: profile?.plan_renewal_at || null,
            avatarPath,
            avatarUrl
        });
    } catch (e) {
        res.status(500).json({ error: e.message || 'Failed to load profile' });
    }
});

router.patch('/profile', requireAuth, async (req, res) => {
    try {
        const displayName = toStringOrEmpty(req.body?.displayName ?? req.body?.display_name);
        if (displayName && displayName.length < 2) {
            return res.status(400).json({ error: 'displayName must be at least 2 characters' });
        }

        const patch = {
            display_name: displayName || null,
            phone: toStringOrEmpty(req.body?.phone) || null,
            address_line1: toStringOrEmpty(req.body?.addressLine1 ?? req.body?.address_line1) || null,
            address_line2: toStringOrEmpty(req.body?.addressLine2 ?? req.body?.address_line2) || null,
            city: toStringOrEmpty(req.body?.city) || null,
            state: toStringOrEmpty(req.body?.state) || null,
            postal_code: toStringOrEmpty(req.body?.postalCode ?? req.body?.postal_code) || null,
            country: toStringOrEmpty(req.body?.country) || null
        };

        const { data, error } = await req.supabase
            .from('user_profiles')
            .upsert({ id: req.user.id, ...patch }, { onConflict: 'id' })
            .select('display_name, phone, address_line1, address_line2, city, state, postal_code, country, plan_name, plan_status, plan_renewal_at, avatar_path')
            .single();

        if (error) throw error;
        const avatarUrl = await buildAvatarSignedUrl(data?.avatar_path);
        return res.status(200).json({
            success: true,
            profile: data,
            userName: toStringOrEmpty(data?.display_name),
            displayName: toStringOrEmpty(data?.display_name),
            avatarUrl
        });
    } catch (e) {
        return res.status(400).json({ error: e.message || 'Failed to update profile' });
    }
});

router.post('/profile/avatar/upload-url', requireAuth, async (req, res) => {
    try {
        const mimeType = toStringOrEmpty(req.body?.mimeType);
        const sizeBytes = Number(req.body?.sizeBytes || 0);
        if (!ALLOWED_MIME.has(mimeType)) {
            return res.status(400).json({ error: 'Only jpg, png, and webp are allowed' });
        }
        if (Number.isFinite(sizeBytes) && sizeBytes > AVATAR_MAX_BYTES) {
            return res.status(400).json({ error: 'File too large (max 5MB)' });
        }

        const ext = inferExt(mimeType);
        const avatarPath = `users/${req.user.id}/avatar-${Date.now()}.${ext}`;
        const { data, error } = await supabaseService.storage
            .from(AVATAR_BUCKET)
            .createSignedUploadUrl(avatarPath);
        if (error) throw error;

        const signedUrl = data?.signedUrl
            ? data.signedUrl.startsWith('http')
                ? data.signedUrl
                : `${process.env.SUPABASE_URL || ''}/storage/v1${data.signedUrl}`
            : '';

        return res.status(200).json({
            bucket: AVATAR_BUCKET,
            avatarPath,
            token: data?.token || '',
            signedUrl
        });
    } catch (e) {
        return res.status(400).json({ error: e.message || 'Could not create upload URL' });
    }
});

router.patch('/profile/avatar', requireAuth, async (req, res) => {
    try {
        const avatarPath = toStringOrEmpty(req.body?.avatarPath ?? req.body?.avatar_path);
        if (!avatarPath) return res.status(400).json({ error: 'avatarPath is required' });
        if (!avatarPath.startsWith(`users/${req.user.id}/`)) {
            return res.status(403).json({ error: 'Invalid avatar path for this user' });
        }

        const { data, error } = await req.supabase
            .from('user_profiles')
            .upsert(
                { id: req.user.id, avatar_path: avatarPath, avatar_updated_at: new Date().toISOString() },
                { onConflict: 'id' }
            )
            .select('avatar_path')
            .single();
        if (error) throw error;
        const avatarUrl = await buildAvatarSignedUrl(data?.avatar_path);
        return res.status(200).json({ success: true, avatarPath: data?.avatar_path || '', avatarUrl });
    } catch (e) {
        return res.status(400).json({ error: e.message || 'Could not save avatar' });
    }
});

router.post('/change-email', requireAuth, async (req, res) => {
    try {
        const newEmail = toStringOrEmpty(req.body?.newEmail ?? req.body?.email);
        if (!newEmail) return res.status(400).json({ error: 'newEmail is required' });
        let data = null;
        let error = null;
        // Preferred path: authenticated user-driven email change (sends confirmation flow).
        const upd = await req.supabase.auth.updateUser({ email: newEmail });
        data = upd.data || null;
        error = upd.error || null;
        // Some server-side contexts return "Auth session missing!" for auth.updateUser.
        // Fallback to admin update so API still works.
        if (error && /auth session missing/i.test(String(error.message || ''))) {
            const adminUpd = await supabaseService.auth.admin.updateUserById(req.user.id, { email: newEmail });
            data = adminUpd.data || null;
            error = adminUpd.error || null;
        }
        if (error) throw error;
        return res.status(200).json({
            success: true,
            message: 'Email updated. Check inbox for any required confirmations.',
            user: data?.user || null
        });
    } catch (e) {
        return res.status(400).json({ error: e.message || 'Could not change email' });
    }
});

router.post('/reset-password', requireAuth, async (req, res) => {
    try {
        const email = toStringOrEmpty(req.user?.email);
        if (!email) return res.status(400).json({ error: 'Missing user email' });
        const redirectTo = toStringOrEmpty(req.body?.redirectTo) || `${toStringOrEmpty(process.env.FRONTEND_URL) || 'http://localhost:5173'}/login`;
        const { error } = await supabase.auth.resetPasswordForEmail(
            email,
            redirectTo ? { redirectTo } : undefined
        );
        if (error) throw error;
        return res.status(200).json({ success: true, message: 'Password reset email sent' });
    } catch (e) {
        return res.status(400).json({ error: e.message || 'Could not send password reset' });
    }
});

router.delete('/account', requireAuth, async (req, res) => {
    try {
        const userId = req.user.id;
        await req.supabase.from('user_profiles').delete().eq('id', userId);
        const { error } = await supabaseService.auth.admin.deleteUser(userId);
        if (error) throw error;
        return res.status(200).json({ success: true, message: 'Account deleted' });
    } catch (e) {
        return res.status(400).json({ error: e.message || 'Could not delete account' });
    }
});

module.exports = router;