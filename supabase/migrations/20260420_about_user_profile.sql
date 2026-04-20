-- About page profile expansion + avatar private storage bucket
-- Run in Supabase SQL editor.

alter table if exists public.user_profiles
  add column if not exists phone text,
  add column if not exists address_line1 text,
  add column if not exists address_line2 text,
  add column if not exists city text,
  add column if not exists state text,
  add column if not exists postal_code text,
  add column if not exists country text,
  add column if not exists plan_name text,
  add column if not exists plan_status text,
  add column if not exists plan_renewal_at timestamptz,
  add column if not exists avatar_path text,
  add column if not exists avatar_updated_at timestamptz,
  add column if not exists updated_at timestamptz not null default now();

create index if not exists user_profiles_plan_status_idx on public.user_profiles (plan_status);
create index if not exists user_profiles_plan_renewal_at_idx on public.user_profiles (plan_renewal_at);

create or replace function public.set_user_profiles_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_user_profiles_updated_at on public.user_profiles;
create trigger trg_user_profiles_updated_at
before update on public.user_profiles
for each row execute function public.set_user_profiles_updated_at();

-- Private avatar bucket for signed upload/download URLs.
insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
  'avatars-private',
  'avatars-private',
  false,
  5242880,
  array['image/jpeg', 'image/png', 'image/webp']
)
on conflict (id) do update
set
  public = excluded.public,
  file_size_limit = excluded.file_size_limit,
  allowed_mime_types = excluded.allowed_mime_types;
