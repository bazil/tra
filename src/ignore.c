#include "tra.h"

static int matchfn(char*, char*);
static char *globpath(char*);

enum
{
	Include,
	Exclude
};

typedef struct Ig Ig;
struct Ig
{
	Ig *next;
	int type;
	int isrooted;
	Apath *ap;
};

Ig *igs;
int didload;

static int
match0(char **pattern, char **path, int n)
{
	int i;

	for(i=0; i<n; i++)
		if(!matchfn(pattern[i], path[i]))
			return 0;
	return 1;
}

static int
match(Ig *ig, Apath *ap)
{
	int i;

	if(ig->ap->n > ap->n)
		return 0;

	if(ig->isrooted)
		return match0(ig->ap->e, ap->e, ig->ap->n);
	else{
		for(i=0; i<=ap->n - ig->ap->n; i++)
			if(match0(ig->ap->e, ap->e+i, ig->ap->n))
				return 1;
		return 0;
	}
}

static void
exc(char *s)
{
	Ig *ig;

	ig = emalloc(sizeof(Ig));
	ig->type = Exclude;
	ig->isrooted = 0;
	ig->ap = mkapath(globpath(s));
	ig->next = igs;
	igs = ig;
}

int
ignorepath(Apath *ap)
{
	Ig *ig;

	dbg(DbgIgnore, "ignore .../%s\n", ap->n ? ap->e[ap->n-1] : "<>");

	if(!didload){
		/* initialize default exclusion list */
		didload = 1;
		exc("*.tradb*");
		exc("minisync.log");
	}
		
	for(ig=igs; ig; ig=ig->next)
		if(match(ig, ap)){
			dbg(DbgIgnore, "match %d\n", ig->type);
			return ig->type==Exclude;
		}

	dbg(DbgIgnore, "no match\n");
	return 0;
}

static char white[] = " \t";

void
loadignore(char *path)
{
	int i;
	char *s, *p;
	Biobuf *b;
	Ig *ig, **l;

fprint(2, "loadignore %s\n", path);
	didload = 1;
	if((b = Bopen(path, OREAD)) == nil)
		sysfatal("open %s: %r", path);

	ig = emalloc(sizeof(Ig));
	l = &igs;
	for(i=1; (s = Brdstr(b, '\n', 1)) != nil; i++, free(s)){
		switch(s[0]){
		case '\0':
		case '#':
			continue;
		}
		p = strpbrk(s, white);
		if(p == nil)
			sysfatal("%s:%d: no argument", path, i);
		*p++ = '\0';
		p += strspn(p, white);
		if(*p == '\0')
			sysfatal("%s:%d: no argument", path, i);

		if(strcmp(s, "include") == 0)
			ig->type = Include;
		else if(strcmp(s, "exclude") == 0)
			ig->type = Exclude;
		else
			sysfatal("%s:%d: bad verb '%s'", path, i, s);

		ig->isrooted = p[0]=='/';
		p = globpath(p);
		if(p == nil)
			sysfatal("%s:%d: syntax error in pattern: %r", path, i);
		ig->ap = mkapath(p);
		*l = ig;
		l = &ig->next;
		ig = emalloc(sizeof(Ig));
	}
	free(ig);
	*l = nil;
	Bterm(b);
}

#define	GLOB	((char)0x01)
static char*
globpath(char *s)
{
	int cclass, inquote;
	char *t, *r, *w;

	inquote = 0;
	cclass = 0;
	t = emalloc(2*strlen(s));
	for(w=t, r=s; *r; r++){
		if(inquote){
			if(*r == '\''){
				if(*(r+1)=='\'')
					*w++ = '\'';
				else
					inquote = 0;
			}else{
				if(*r == ']')
					cclass = 0;
				*w++ = *r;
			}
		}else{
			switch(*r){
			case '\'':
				inquote = 1;
				break;
			case '[':
				cclass = 1;
				/* fall through */
			case GLOB:
			case '*':
			case '?':
				/* fall through */
				*w++ = GLOB;
			default:
				if(*r == ']')
					cclass = 0;
				*w++ = *r;
			}
		}
	}
	*w = '\0';
	if(inquote){
		free(t);
		werrstr("no closing quote");
		return nil;
	}
	if(cclass){
		free(t);
		werrstr("no closing bracket");
		return nil;
	}
	return t;
}

/*
 * The rest of this file is from Plan 9's rc, and is covered by the
 * Plan 9 open source license.  See libc/LICENSE.
 */
/*
 * Glob character escape in strings:
 *	In a string, GLOB must be followed by *?[ or GLOB.
 *	GLOB* matches any string
 *	GLOB? matches any single character
 *	GLOB[...] matches anything in the brackets
 *	GLOBGLOB matches GLOB
 */
/*
 * onebyte(c), twobyte(c), threebyte(c)
 * Is c the first character of a one- two- or three-byte utf sequence?
 */
#define	onebyte(c)	((c&0x80)==0x00)
#define	twobyte(c)	((c&0xe0)==0xc0)
#define	threebyte(c)	((c&0xf0)==0xe0)
/*
 * Do p and q point at equal utf codes
 */
static int equtf(char *p, char *q){
	if(*p!=*q) return 0;
	if(twobyte(*p)) return p[1]==q[1];
	if(threebyte(*p)){
		if(p[1]!=q[1]) return 0;
		if(p[1]=='\0') return 1;	/* broken code at end of string! */
		return p[2]==q[2];
	}
	return 1;
}
/*
 * Return a pointer to the next utf code in the string,
 * not jumping past nuls in broken utf codes!
 */
static char *nextutf(char *p){
	if(twobyte(*p)) return p[1]=='\0'?p+1:p+2;
	if(threebyte(*p)) return p[1]=='\0'?p+1:p[2]=='\0'?p+2:p+3;
	return p+1;
}
/*
 * Convert the utf code at *p to a unicode value
 */
static int unicode(char *p){
	int u=*p&0xff;
	if(twobyte(u)) return ((u&0x1f)<<6)|(p[1]&0x3f);
	if(threebyte(u)) return (u<<12)|((p[1]&0x3f)<<6)|(p[2]&0x3f);
	return u;
}
/*
 * Does the string s match the pattern p
 * . and .. are only matched by patterns starting with .
 * * matches any sequence of characters
 * ? matches any single character
 * [...] matches the enclosed list of characters
 */
static int matchel(char *s, char *p, int stop)
{
	int compl, hit, lo, hi, t, c;
	for(;*p!=stop && *p!='\0';s=nextutf(s),p=nextutf(p)){
		if(*p!=GLOB){
			if(!equtf(p, s)) return 0;
		}
		else switch(*++p){
		case GLOB:
			if(*s!=GLOB) return 0;
			break;
		case '*':
			for(;;){
				if(matchel(s, nextutf(p), stop)) return 1;
				if(!*s) break;
				s=nextutf(s);
			}
			return 0;
		case '?':
			if(*s=='\0') return 0;
			break;
		case '[':
			if(*s=='\0') return 0;
			c=unicode(s);
			p++;
			compl=*p=='~';
			if(compl) p++;
			hit=0;
			while(*p!=']'){
				if(*p=='\0') return 0;		/* syntax error */
				lo=unicode(p);
				p=nextutf(p);
				if(*p!='-') hi=lo;
				else{
					p++;
					if(*p=='\0') return 0;	/* syntax error */
					hi=unicode(p);
					p=nextutf(p);
					if(hi<lo){ t=lo; lo=hi; hi=t; }
				}
				if(lo<=c && c<=hi) hit=1;
			}
			if(compl) hit=!hit;
			if(!hit) return 0;
			break;
		}
	}
	return *s=='\0';
}
static int matchfn(char *p, char *s)
{
	int i;
	if(s[0]=='.' && (s[1]=='\0' || (s[1]=='.' && s[2]=='\0')) && p[0]!='.')
		return 0;
	i = matchel(s, p, '/');
/*fprint(2, "matchfn %s %s => %d\n", s, p, i);*/
	return i;
}
